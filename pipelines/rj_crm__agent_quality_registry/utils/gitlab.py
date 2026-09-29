"""Acesse os artefatos de qualidade no GitLab Generic Registry."""

import hashlib
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.rj_crm__agent_quality_registry.constants import (
    GITLAB_PACKAGE_PROD,
    GITLAB_PACKAGE_QA,
    GITLAB_PROJECT_ID,
    GITLAB_TOKEN,
    GITLAB_URL,
)
from pipelines.rj_crm__agent_quality_registry.utils.schemas import Artifact, RegistryFile


class ArtifactChecksumError(ValueError):
    """Indique que o conteúdo baixado não corresponde ao SHA256 do Registry."""


class GitLabRegistryClient:
    """Leia arquivos publicados no GitLab Generic Registry."""

    def __init__(self, base_url: str, project_id: str, token: str, timeout: int = 60) -> None:
        """Inicialize o cliente autenticado do GitLab Registry.

        :param base_url: URL base da instância GitLab.
        :param project_id: Identificador do projeto GitLab.
        :param token: Token privado com permissão de leitura do Registry.
        :param timeout: Tempo máximo de espera de cada requisição, em segundos.
        :raises ValueError: Se ``token`` estiver vazio.
        """
        if not token:
            raise ValueError("AGENT_QUALITY_GITLAB_TOKEN não configurado")
        self.base_url = base_url.rstrip("/")
        self.project_id = quote(str(project_id), safe="")
        self.session = requests.Session()
        self.session.headers.update({"PRIVATE-TOKEN": token, "Accept": "application/json"})
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.timeout = timeout

    def get(self, path: str, **params: Any) -> Any:
        """Obtenha e decodifique uma resposta JSON da API GitLab.

        :param path: Caminho da API após ``/api/v4``.
        :param params: Parâmetros de query enviados à API.
        :returns: Corpo JSON decodificado.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        response = self.session.get(
            f"{self.base_url}/api/v4{path}",
            params=params or None,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def list_files(self, package_name: str, environment: str) -> list[RegistryFile]:
        """Liste os arquivos de artefato de um pacote do Registry.

        :param package_name: Nome do pacote Generic Registry.
        :param environment: Ambiente associado ao pacote.
        :returns: Arquivos ``agent-quality-artifact.json`` publicados.
        :raises requests.HTTPError: Se a API responder com erro HTTP.
        """
        packages: list[dict[str, Any]] = []
        page = 1
        page_size = 100
        while True:
            batch = self.get(
                f"/projects/{self.project_id}/packages",
                package_type="generic",
                package_name=package_name,
                per_page=page_size,
                page=page,
            )
            packages.extend(batch)
            if len(batch) < page_size:
                break
            page += 1

        files: list[RegistryFile] = []
        for package in packages:
            if package.get("name") != package_name:
                continue
            package_id = package.get("id")
            version = str(package.get("version", ""))
            if not package_id or not version:
                continue
            package_files: list[dict[str, Any]] = []
            page = 1
            while True:
                batch = self.get(
                    f"/projects/{self.project_id}/packages/{package_id}/package_files",
                    per_page=page_size,
                    page=page,
                )
                package_files.extend(batch)
                if len(batch) < page_size:
                    break
                page += 1
            for package_file in package_files:
                file_name = package_file.get("file_name")
                if file_name != "agent-quality-artifact.json":
                    continue
                package_file_id = int(package_file["id"])
                files.append(
                    RegistryFile(
                        environment=environment,
                        package_name=package_name,
                        package_version=version,
                        package_id=int(package_id),
                        package_file_id=package_file_id,
                        file_name=file_name,
                        file_sha256=package_file.get("file_sha256"),
                        created_at=package_file.get("created_at") or package.get("created_at"),
                        download_url=(
                            f"{self.base_url}/api/v4/projects/{self.project_id}/packages/generic/"
                            f"{quote(package_name, safe='')}/{quote(version, safe='')}/"
                            f"{quote(file_name, safe='')}"
                        ),
                    )
                )
        latest_by_coordinate: dict[tuple[str, str, str], RegistryFile] = {}
        for registry_file in files:
            coordinate = (
                registry_file.package_name,
                registry_file.package_version,
                registry_file.file_name,
            )
            current = latest_by_coordinate.get(coordinate)
            if current is None or registry_file.package_file_id > current.package_file_id:
                latest_by_coordinate[coordinate] = registry_file
        return list(latest_by_coordinate.values())

    def download(self, registry_file: RegistryFile) -> dict[str, Any]:
        """Baixe e decodifique um artefato do Registry.

        :param registry_file: Metadados do arquivo a baixar.
        :returns: Conteúdo JSON do artefato.
        :raises requests.HTTPError: Se o download responder com erro HTTP.
        """
        response = self.session.get(registry_file.download_url, timeout=self.timeout)
        response.raise_for_status()
        actual_sha256 = hashlib.sha256(response.content).hexdigest()
        if registry_file.file_sha256 and actual_sha256.lower() != registry_file.file_sha256.lower():
            raise ArtifactChecksumError(
                "SHA256 divergente para "
                f"package_file_id={registry_file.package_file_id}: "
                f"esperado={registry_file.file_sha256}, obtido={actual_sha256}"
            )
        return response.json()


def create_gitlab_client() -> GitLabRegistryClient:
    """Crie o cliente do Registry com a configuração do ambiente."""
    return GitLabRegistryClient(GITLAB_URL, GITLAB_PROJECT_ID, GITLAB_TOKEN)


def discover_registry_files(client: GitLabRegistryClient | None = None) -> list[RegistryFile]:
    """Descubra os arquivos de QA e baseline sem baixar seus conteúdos."""
    registry_client = client or create_gitlab_client()
    files = registry_client.list_files(GITLAB_PACKAGE_QA, "qa")
    files.extend(registry_client.list_files(GITLAB_PACKAGE_PROD, "prod_baseline"))
    return files


def discover_artifacts() -> list[Artifact]:
    """Descubra e baixe os artefatos de QA e de baseline de produção.

    :returns: Pares de metadados do Registry e conteúdo de artefato.
    :raises ValueError: Se o token do GitLab não estiver configurado.
    :raises requests.HTTPError: Se a API GitLab responder com erro HTTP.
    """
    client = create_gitlab_client()
    files = discover_registry_files(client)
    return [(registry_file, client.download(registry_file)) for registry_file in files]
