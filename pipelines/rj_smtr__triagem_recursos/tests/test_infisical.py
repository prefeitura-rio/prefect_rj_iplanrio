from json import loads
from pathlib import Path

import httpx
import pytest
from httpx import MockTransport, Request, Response

from pipelines.rj_smtr__triagem_recursos.helpers import infisical
from pipelines.rj_smtr__triagem_recursos.tests.conftest import json_response


def test_montar_paths_infisical_normalizes_and_deduplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Garante normalização de barra inicial, ordem do override e remoção de duplicatas."""
    monkeypatch.setenv("INFISICAL_PATHS", " /rj_smtr__triagem_recursos , /shared , /rj_smtr__triagem_recursos ")

    assert infisical.montar_paths_infisical("rj_smtr__triagem_recursos") == [
        "/rj_smtr__triagem_recursos",
        "/shared",
    ]
    assert infisical.montar_paths_infisical("/") == [
        "/rj_smtr__triagem_recursos",
        "/shared",
    ]


def test_carregar_secrets_infisical_skips_when_credentials_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Em execução local sem credenciais Infisical configuradas, retorna lista vazia sem falhar."""
    for key in (
        "INFISICAL_URL",
        "INFISICAL_PROJECT_ID",
        "INFISICAL_CLIENT_ID",
        "INFISICAL_CLIENT_SECRET",
        "INFISICAL_IDENTITY_ID",
    ):
        monkeypatch.delenv(key, raising=False)

    assert infisical.carregar_secrets_infisical_se_necessario() == []


def test_carregar_secrets_universal_auth_merges_paths_and_ignores_404_secondary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Carrega segredos via Universal Auth, respeita precedência de paths e preserva env já definida."""
    requests_seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: Request) -> Response:
        requests_seen.append((request.method, request.url.path, dict(request.url.params)))
        if request.url.path == infisical.INFISICAL_UNIVERSAL_LOGIN_PATH:
            body = loads(request.content.decode("utf-8"))
            assert body == {"clientId": "cid-1", "clientSecret": "csec-1"}
            return json_response({"accessToken": "tok-ua"})
        if request.url.path == infisical.INFISICAL_SECRETS_PATH:
            assert request.headers["Authorization"] == "Bearer tok-ua"
            secret_path = request.url.params.get("secretPath")
            if secret_path == "/rj_smtr__triagem_recursos":
                return json_response(
                    {
                        "secrets": [
                            {"secretKey": "DATABASE_URL", "secretValue": "postgres://primary"},
                            {"secretKey": "PROMPT_SISTEMA_V4", "secretValue": "prompt-v4"},
                        ]
                    }
                )
            if secret_path == "/shared":
                return json_response(
                    {
                        "secrets": [
                            {"secretKey": "DATABASE_URL", "secretValue": "postgres://shared-ignored"},
                            {"secretKey": "BIFROST_BASE_URL", "secretValue": "https://bifrost.local"},
                        ]
                    }
                )
            if secret_path == "/opcional-ausente":
                return json_response({"message": "Not found"}, status_code=404)
        return json_response({"message": "Unexpected"}, status_code=500)

    monkeypatch.setenv("INFISICAL_URL", "https://secrets.dados.rio/")
    monkeypatch.setenv("INFISICAL_PROJECT_ID", "proj-123")
    monkeypatch.setenv("INFISICAL_ENVIRONMENT", "stg")
    monkeypatch.setenv("INFISICAL_PATHS", "/shared,/opcional-ausente")
    monkeypatch.setenv("INFISICAL_CLIENT_ID", "cid-1")
    monkeypatch.setenv("INFISICAL_CLIENT_SECRET", "csec-1")
    monkeypatch.setenv("BIFROST_BASE_URL", "https://already-in-pod")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PROMPT_SISTEMA_V4", raising=False)

    loaded = infisical.carregar_secrets_infisical_se_necessario(
        secret_path_override="/rj_smtr__triagem_recursos",
        transport=MockTransport(handler),
    )

    assert loaded == ["BIFROST_BASE_URL", "DATABASE_URL", "PROMPT_SISTEMA_V4"]
    assert infisical.os.environ["DATABASE_URL"] == "postgres://primary"
    assert infisical.os.environ["PROMPT_SISTEMA_V4"] == "prompt-v4"
    assert infisical.os.environ["BIFROST_BASE_URL"] == "https://already-in-pod"
    assert [r[2].get("secretPath") for r in requests_seen[1:]] == [
        "/rj_smtr__triagem_recursos",
        "/shared",
        "/opcional-ausente",
    ]


def test_carregar_secrets_kubernetes_auth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Autentica via Kubernetes ServiceAccount JWT quando INFISICAL_IDENTITY_ID estiver presente."""
    sa_token_file = tmp_path / "token"
    sa_token_file.write_text("k8s-jwt-token\n", encoding="utf-8")
    monkeypatch.setattr(infisical, "SERVICE_ACCOUNT_TOKEN_PATH", sa_token_file)

    def handler(request: Request) -> Response:
        if request.url.path == infisical.INFISICAL_K8S_LOGIN_PATH:
            body = loads(request.content.decode("utf-8"))
            assert body == {"identityId": "id-k8s", "jwt": "k8s-jwt-token"}
            return json_response({"accessToken": "tok-k8s"})
        if request.url.path == infisical.INFISICAL_SECRETS_PATH:
            return json_response({"secrets": [{"secretKey": "SEI_ORGAO", "secretValue": "28"}]})
        return json_response({}, status_code=404)

    monkeypatch.setenv("INFISICAL_URL", "https://secrets.dados.rio")
    monkeypatch.setenv("INFISICAL_PROJECT_ID", "proj-123")
    monkeypatch.setenv("INFISICAL_IDENTITY_ID", "id-k8s")
    monkeypatch.delenv("INFISICAL_CLIENT_ID", raising=False)
    monkeypatch.delenv("INFISICAL_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("INFISICAL_PATHS", raising=False)
    monkeypatch.delenv("SEI_ORGAO", raising=False)

    loaded = infisical.carregar_secrets_infisical_se_necessario(
        secret_path_override="/rj_smtr__triagem_recursos",
        transport=MockTransport(handler),
    )

    assert loaded == ["SEI_ORGAO"]
    assert infisical.os.environ["SEI_ORGAO"] == "28"


def test_carregar_secrets_raises_on_primary_path_404(monkeypatch: pytest.MonkeyPatch) -> None:
    """Falha explicitamente quando a pasta principal (idx == 0) retorna 404."""

    def handler(request: Request) -> Response:
        if request.url.path == infisical.INFISICAL_UNIVERSAL_LOGIN_PATH:
            return json_response({"accessToken": "tok-ua"})
        return json_response({"message": "Not found"}, status_code=404)

    monkeypatch.setenv("INFISICAL_URL", "https://secrets.dados.rio")
    monkeypatch.setenv("INFISICAL_PROJECT_ID", "proj-123")
    monkeypatch.setenv("INFISICAL_CLIENT_ID", "cid-1")
    monkeypatch.setenv("INFISICAL_CLIENT_SECRET", "csec-1")

    with pytest.raises(httpx.HTTPStatusError):
        infisical.carregar_secrets_infisical_se_necessario(
            secret_path_override="/pasta-inexistente",
            transport=MockTransport(handler),
        )
