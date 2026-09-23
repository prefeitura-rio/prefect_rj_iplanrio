"""Defina os modelos internos da ingestão de qualidade."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RegistryFile:
    """Represente um arquivo de artefato no GitLab Generic Registry."""

    environment: str
    package_name: str
    package_version: str
    package_id: int
    package_file_id: int
    file_name: str
    file_sha256: str | None
    created_at: str | None
    download_url: str


Artifact = tuple[RegistryFile, dict[str, Any]]


@dataclass(frozen=True)
class IngestionConfig:
    """Agrupe a configuração de destino de uma execução da ingestão."""

    project_id: str
    dataset_id: str
    environment: str


@dataclass(frozen=True)
class TableSpec:
    """Agrupe a definição de uma tabela de destino BigQuery."""

    name: str
    fields: list[tuple[str, str, str]]
    partition_field: str
    key: str


@dataclass(frozen=True)
class GridConfig:
    """Agrupe as credenciais e o endpoint do Salesforce Test Grid."""

    instance_url: str
    access_token: str
    client_id: str
    client_secret: str
    token_endpoint: str
    api_version: str
