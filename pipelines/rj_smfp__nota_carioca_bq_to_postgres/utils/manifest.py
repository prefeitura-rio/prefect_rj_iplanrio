"""Leitura do manifesto do export, gravado pelo ``bq_to_oracle`` em ``gs://<bucket>/<dataset>/<flow_run_id>/``.

O contrato é o de ``rj_smfp__nota_carioca_bq_to_oracle/utils/manifest.py`` (cada imagem Docker copia só a sua pasta);
mude os dois juntos. Os arquivos são nomes de objetos no mesmo bucket do manifesto.
"""

import re
from dataclasses import dataclass

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, NonNegativeInt

MANIFEST_URI = re.compile(r"^gs://(?P<bucket>[^/]+)/(?P<dataset>[^/]+)/(?P<run>[^/]+)/manifesto\.json$")


@dataclass(frozen=True)
class ManifestLocation:
    """Onde está o manifesto.

    :param bucket: Bucket do export.
    :param name: Nome do objeto do manifesto no bucket.
    :param dataset_id: Dataset de origem, primeiro nível do prefixo.
    """

    bucket: str
    name: str
    dataset_id: str


def parse_manifest_uri(uri: str) -> ManifestLocation:
    """Separa o URI ``gs://<bucket>/<dataset>/<flow_run_id>/manifesto.json``.

    :param uri: URI do manifesto.
    :returns: Bucket, objeto e dataset.
    :raises ValueError: Se o URI não for o de um manifesto de um flow run.
    """
    match = MANIFEST_URI.match(uri)
    if match is None:
        raise ValueError(
            f"URI de manifesto inválido: {uri!r}; esperado gs://<bucket>/<dataset>/<flow_run_id>/manifesto.json"
        )
    return ManifestLocation(
        bucket=match["bucket"], name=f"{match['dataset']}/{match['run']}/manifesto.json", dataset_id=match["dataset"]
    )


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)


class ExportedFile(_Frozen):
    """Um ``part-*.csv.gz`` do export."""

    name: str
    size: NonNegativeInt


class BqField(_Frozen):
    """Um campo do schema do BigQuery, na ordem das colunas do CSV."""

    name: str
    type: str
    mode: str


class TableSchema(_Frozen):
    """Schema da tabela na foto exportada."""

    fields: tuple[BqField, ...]
    num_rows: NonNegativeInt


class TableExport(_Frozen):
    """Uma tabela exportada: arquivos, schema, linhas e a última alteração dela no BigQuery."""

    files: tuple[ExportedFile, ...]
    bq_schema: TableSchema = Field(alias="schema")
    rows: NonNegativeInt = Field(alias="linhas")
    last_modified: AwareDatetime | None = None

    @property
    def fields(self) -> tuple[BqField, ...]:
        """Campos do CSV, na ordem do schema do BigQuery."""
        return self.bq_schema.fields


class Manifest(_Frozen):
    """O export de um flow run do ``bq_to_oracle``.

    ``snapshot_modified`` é a foto do BigQuery que o Oracle recebeu (a do comentário da tabela BQLOAD dos detalhes) e
    que ``publicacoes.snapshot_bq`` repete.
    """

    flow_run_id: str = Field(min_length=1)
    tables: dict[str, TableExport] = Field(alias="tabelas")
    snapshot_modified: AwareDatetime


def parse_manifest(text: str) -> Manifest:
    """Lê o JSON do manifesto.

    :param text: Conteúdo do ``manifesto.json``.
    :returns: O manifesto tipado.
    :raises pydantic.ValidationError: Se o JSON não tiver o formato do contrato.
    """
    return Manifest.model_validate_json(text)
