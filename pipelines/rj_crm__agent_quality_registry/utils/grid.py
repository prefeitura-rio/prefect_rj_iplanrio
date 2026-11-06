"""Acesse dados opcionais de enriquecimento no Salesforce Test Grid."""

from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from pipelines.rj_crm__agent_quality_registry.constants import (
    SF_ACCESS_TOKEN,
    SF_API_VERSION,
    SF_CLIENT_ID,
    SF_CLIENT_SECRET,
    SF_INSTANCE_URL,
    SF_TOKEN_ENDPOINT,
)
from pipelines.rj_crm__agent_quality_registry.utils.schemas import GridConfig


class TestGridClient:
    """Leia execuções e planilhas do Salesforce Test Grid."""

    def __init__(self, config: GridConfig) -> None:
        """Inicialize o cliente e obtenha um token quando possível.

        :param config: Credenciais, endpoint e versão de API do Grid.
        :raises requests.HTTPError: Se a autenticação OAuth responder com erro HTTP.
        :raises RuntimeError: Se o OAuth não retornar um token de acesso.
        """
        self.base_url = config.instance_url.rstrip("/")
        self.api_version = config.api_version if config.api_version.startswith("v") else f"v{config.api_version}"
        self.session = requests.Session()
        client_token = self.get_client_credentials_token(
            client_id=config.client_id,
            client_secret=config.client_secret,
            token_endpoint=config.token_endpoint or f"{self.base_url}/services/oauth2/token",
        )
        self.access_token = client_token or config.access_token
        self.enabled = bool(self.base_url and self.access_token)
        if self.enabled:
            self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})

    @staticmethod
    def get_client_credentials_token(
        client_id: str,
        client_secret: str,
        token_endpoint: str,
    ) -> str:
        """Obtenha um token OAuth Client Credentials quando configurado.

        :param client_id: Client ID OAuth.
        :param client_secret: Client secret OAuth.
        :param token_endpoint: Endpoint OAuth.
        :returns: Token de acesso ou string vazia quando OAuth não foi configurado.
        :raises requests.HTTPError: Se o endpoint OAuth responder com erro HTTP.
        :raises RuntimeError: Se o endpoint não retornar ``access_token``.
        """
        if not (client_id and client_secret and token_endpoint):
            return ""
        response = requests.post(
            token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=60,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("Salesforce não retornou access_token no fluxo Client Credentials")
        return str(token)

    def get(self, path: str) -> Any:
        """Obtenha e decodifique uma resposta da API Salesforce.

        :param path: Caminho após a versão da API Salesforce.
        :returns: Corpo JSON decodificado.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        response = self.session.get(
            f"{self.base_url}/services/data/{self.api_version}{path}",
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def items(payload: Any) -> list[dict[str, Any]]:
        """Extraia uma lista de itens dos formatos conhecidos da API.

        :param payload: Corpo JSON retornado pela API.
        :returns: Itens encontrados ou lista vazia.
        """
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("records", "items", "data", "workbooks", "worksheets"):
            if isinstance(payload.get(key), list):
                return payload[key]
        return []

    def fetch_run(self, run_id: str) -> dict[str, Any]:
        """Busque o estado de uma execução do Test Grid.

        :param run_id: Identificador da execução.
        :returns: Estado da execução.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        return self.get(f"/einstein/ai-testing/runs/{quote(run_id, safe='')}")

    def fetch_worksheet_data(self, worksheet_id: str) -> dict[str, Any]:
        """Busque os dados de uma planilha do Test Grid.

        :param worksheet_id: Identificador da planilha.
        :returns: Dados da planilha.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        return self.get(f"/public/grid/worksheets/{quote(worksheet_id, safe='')}/data")

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    @classmethod
    def _run_window(cls, run_status: dict[str, Any] | None) -> tuple[datetime | None, datetime | None]:
        payload = run_status or {}
        start = next(
            (
                cls._parse_datetime(payload.get(key))
                for key in ("runStart", "startTime", "startedAt", "createdAt", "createdDate")
                if cls._parse_datetime(payload.get(key))
            ),
            None,
        )
        end = next(
            (
                cls._parse_datetime(payload.get(key))
                for key in ("runEnd", "endTime", "completedAt", "finishedAt", "lastModifiedAt")
                if cls._parse_datetime(payload.get(key))
            ),
            None,
        )
        return start, end

    def find_worksheet(
        self,
        suite_name: str,
        run_status: dict[str, Any] | None = None,
    ) -> tuple[str, str] | None:
        """Encontre uma planilha cujo nome corresponda ao da suite.

        :param suite_name: Nome da suite executada.
        :returns: IDs do workbook e da planilha, ou ``None`` se não encontrados.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        normalized = suite_name.strip().lower()
        if not normalized:
            return None
        run_start, run_end = self._run_window(run_status)
        workbooks = self.items(self.get("/public/grid/workbooks"))
        candidates: list[tuple[tuple[int, int, float], str, str]] = []
        for workbook in workbooks:
            workbook_id = workbook.get("id") or workbook.get("workbookId")
            if not workbook_id:
                continue
            workbook_names = [
                str(workbook.get(key, "")).strip().lower() for key in ("name", "label", "title", "apiName")
            ]
            workbook_matches = any(normalized == name or normalized in name for name in workbook_names)
            worksheets = self.items(self.get(f"/public/grid/workbooks/{workbook_id}/worksheets"))
            for worksheet in worksheets:
                worksheet_id = worksheet.get("id") or worksheet.get("worksheetId")
                names = [str(worksheet.get(key, "")).strip().lower() for key in ("name", "label", "title", "apiName")]
                matching_names = [name for name in names if normalized == name or normalized in name]
                if not worksheet_id or not matching_names:
                    continue
                exact_penalty = 0 if normalized in matching_names else 1
                workbook_penalty = 0 if workbook_matches else 1
                worksheet_time = next(
                    (
                        self._parse_datetime(worksheet.get(key))
                        for key in ("createdAt", "createdDate", "lastModifiedAt", "updatedAt")
                        if self._parse_datetime(worksheet.get(key))
                    ),
                    None,
                )
                temporal_penalty = float("inf")
                if worksheet_time and run_start:
                    if run_end and run_start <= worksheet_time <= run_end:
                        temporal_penalty = 0
                    else:
                        temporal_penalty = abs((worksheet_time - run_start).total_seconds())
                candidates.append(
                    (
                        (exact_penalty, workbook_penalty, temporal_penalty),
                        str(workbook_id),
                        str(worksheet_id),
                    )
                )
        if not candidates:
            return None
        _, workbook_id, worksheet_id = min(candidates, key=lambda candidate: candidate[0])
        return workbook_id, worksheet_id

    def enrich(self, suite_name: str, run_id: str) -> dict[str, Any] | None:
        """Obtenha os IDs de planilha e workbook para uma suite.

        :param suite_name: Nome da suite executada.
        :param run_id: Identificador da execução do Test Grid.
        :returns: Dados de enriquecimento ou ``None`` quando o cliente está desabilitado.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        if not self.enabled or not run_id:
            return None
        run_status = self.fetch_run(run_id)
        resolved = self.find_worksheet(suite_name, run_status)
        if not resolved:
            return {"run_status": run_status, "enrichment_status": "WORKSHEET_NOT_FOUND"}
        workbook_id, worksheet_id = resolved
        return {
            "run_id": run_id,
            "run_status": run_status,
            "workbook_id": workbook_id,
            "worksheet_id": worksheet_id,
            "worksheet_data": self.fetch_worksheet_data(worksheet_id),
            "enrichment_status": "SUCCESS",
        }

    @classmethod
    def worksheet_rows(cls, payload: Any) -> list[dict[str, Any]]:
        """Extraia linhas dos formatos conhecidos da resposta de worksheet."""
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("rows", "records", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        data = payload.get("data")
        if data is not payload:
            return cls.worksheet_rows(data)
        return []

    @staticmethod
    def normalize_worksheet_row(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize uma linha de worksheet para os campos de detalhe conhecidos."""
        flattened = dict(row)
        cells = row.get("cells")
        if isinstance(cells, list):
            for cell in cells:
                if not isinstance(cell, dict):
                    continue
                name = cell.get("name") or cell.get("column") or cell.get("field")
                if name:
                    flattened.setdefault(str(name), cell.get("value"))

        def first(*keys: str) -> Any:
            return next((flattened[key] for key in keys if flattened.get(key) is not None), None)

        return {
            "worksheet_row_id": first("id", "rowId", "worksheetRowId"),
            "case_number": first("caseNumber", "case_number", "testCaseNumber", "caseId"),
            "assertion": first("assertion", "assertionName", "metricName", "metric"),
            "status": first("status", "result", "outcome"),
            "score": first("score", "metricScore"),
            "latency_ms": first("latencyMs", "latency_ms", "latency"),
            "expected_value": first("expectedValue", "expected_value", "expected"),
            "actual_value": first("actualValue", "actual_value", "actual"),
            "message": first("message", "error", "rationale"),
            "service_name": first("expectedSubagent", "serviceName", "service_name"),
            "utterance": first("utterance", "input", "question"),
            "response": first("response", "output", "answer"),
        }


def create_grid_client() -> TestGridClient:
    """Crie o cliente Test Grid com as credenciais do ambiente.

    :returns: Cliente Test Grid configurado.
    :raises requests.HTTPError: Se a autenticação OAuth responder com erro HTTP.
    :raises RuntimeError: Se o OAuth não retornar um token de acesso.
    """
    return TestGridClient(
        GridConfig(
            instance_url=SF_INSTANCE_URL,
            access_token=SF_ACCESS_TOKEN,
            client_id=SF_CLIENT_ID,
            client_secret=SF_CLIENT_SECRET,
            token_endpoint=SF_TOKEN_ENDPOINT,
            api_version=SF_API_VERSION,
        )
    )


def create_disabled_grid_client() -> TestGridClient:
    """Crie um cliente explicitamente desabilitado para fallback seguro."""
    return TestGridClient(
        GridConfig(
            instance_url="",
            access_token="",
            client_id="",
            client_secret="",
            token_endpoint="",
            api_version=SF_API_VERSION,
        )
    )
