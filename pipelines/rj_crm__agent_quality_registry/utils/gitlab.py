"""Acesse os artefatos de qualidade no GitLab Generic Registry."""

from typing import Any
from urllib.parse import quote

import requests

from pipelines.rj_crm__agent_quality_registry.constants import (
    GITLAB_PACKAGE_PROD,
    GITLAB_PACKAGE_QA,
    GITLAB_PROJECT_ID,
    GITLAB_TOKEN,
    GITLAB_URL,
)
from pipelines.rj_crm__agent_quality_registry.utils.schemas import Artifact, RegistryFile


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
            package_id = package.get("id")
            version = str(package.get("version", ""))
            if not package_id or not version:
                continue
            package_files = self.get(f"/projects/{self.project_id}/packages/{package_id}/package_files")
            for package_file in package_files:
                file_name = package_file.get("file_name")
                if file_name != "agent-quality-artifact.json":
                    continue
                files.append(
                    RegistryFile(
                        environment=environment,
                        package_name=package_name,
                        package_version=version,
                        package_id=int(package_id),
                        package_file_id=int(package_file["id"]),
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
        return files

    def download(self, registry_file: RegistryFile) -> dict[str, Any]:
        """Baixe e decodifique um artefato do Registry.

        :param registry_file: Metadados do arquivo a baixar.
        :returns: Conteúdo JSON do artefato.
        :raises requests.HTTPError: Se o download responder com erro HTTP.
        """
        response = self.session.get(registry_file.download_url, timeout=self.timeout)
        response.raise_for_status()
        return response.json()


def discover_artifacts() -> list[Artifact]:
    """Descubra e baixe os artefatos de QA e de baseline de produção.

    :returns: Pares de metadados do Registry e conteúdo de artefato.
    :raises ValueError: Se o token do GitLab não estiver configurado.
    :raises requests.HTTPError: Se a API GitLab responder com erro HTTP.
    """
    client = GitLabRegistryClient(GITLAB_URL, GITLAB_PROJECT_ID, GITLAB_TOKEN)
    files = client.list_files(GITLAB_PACKAGE_QA, "qa")
    files.extend(client.list_files(GITLAB_PACKAGE_PROD, "prod_baseline"))
    return [(registry_file, client.download(registry_file)) for registry_file in files]
