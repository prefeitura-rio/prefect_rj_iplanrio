"""Porta do índice e do cursor e adaptador do BigQuery (``raw_nfse_nacional``).

Tabelas (o dataset já deve existir; as tabelas são criadas na primeira execução):

- ``documentos(nsu INT64, chave_acesso, tipo_documento, tipo_evento, data_hora_geracao TIMESTAMP, gcs_uri, linha INT64,
  xml_sha256, origem, ambiente, ingerido_em)``: particionada por ``DATE(data_hora_geracao)``, cluster por ``nsu``,
  ``require_partition_filter=true``, escrita **só por load job** de NDJSON (nunca streaming insert).
- ``adn_cursor(ambiente, ultimo_nsu INT64, atualizado_em TIMESTAMP)``: um registro por ambiente, só avança.
- ``adn_nsu_inexistente(ambiente, nsu INT64, verificado_em TIMESTAMP)``: NSUs que o ADN confirmou não ter; saem da
  lista de lacunas. Para voltar a buscar um deles, apague a linha.
"""

import io
import json
from collections.abc import Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Protocol

from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_adn.constants import (
    DATASET_RAW,
    PROJETO_GCP,
    TABELA_CURSOR,
    TABELA_DOCUMENTOS,
    TABELA_INEXISTENTES,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import DocumentoIndexado, FaixaNsu, LinhaIndice
from prefect_rj_iplanrio.sql import load_query

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)

SCHEMA_DOCUMENTOS = [
    bigquery.SchemaField("nsu", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("chave_acesso", "STRING"),
    bigquery.SchemaField("tipo_documento", "STRING"),
    bigquery.SchemaField("tipo_evento", "STRING"),
    bigquery.SchemaField("data_hora_geracao", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("linha", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("xml_sha256", "STRING"),
    bigquery.SchemaField("origem", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ambiente", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingerido_em", "TIMESTAMP", mode="REQUIRED"),
]
SCHEMA_CURSOR = [
    bigquery.SchemaField("ambiente", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ultimo_nsu", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("atualizado_em", "TIMESTAMP", mode="REQUIRED"),
]
SCHEMA_INEXISTENTES = [
    bigquery.SchemaField("ambiente", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("nsu", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("verificado_em", "TIMESTAMP", mode="REQUIRED"),
]


class Indice(Protocol):
    """O que o extrator e o backfill precisam do índice, do cursor e do registro de NSUs inexistentes."""

    def garantir_tabelas(self) -> None:
        """Cria as tabelas que faltarem."""
        ...

    def ler_cursor(self, ambiente: str) -> int | None:
        """Último NSU gravado do ambiente, ou ``None`` se nunca houve cursor."""
        ...

    def gravar_cursor(self, ambiente: str, ultimo_nsu: int, atualizado_em: datetime) -> None:
        """Cria o cursor ou o avança; nunca o faz recuar."""
        ...

    def nsus_indexados(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> set[int]:
        """NSUs do ambiente já indexados em ``[nsu_ini, nsu_fim]``, de qualquer origem."""
        ...

    def documentos_na_faixa(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> list[DocumentoIndexado]:
        """NSU, hash do XML, origem e posição no raw de cada linha indexada em ``[nsu_ini, nsu_fim]``."""
        ...

    def carregar_documentos(self, linhas: Sequence[LinhaIndice]) -> None:
        """Acrescenta linhas ao índice por load job."""
        ...

    def carregar_inexistentes(self, ambiente: str, nsus: Sequence[int], verificado_em: datetime) -> None:
        """Registra NSUs que o ADN confirmou não ter."""
        ...

    def lacunas(self, ambiente: str, limite: int) -> list[FaixaNsu]:
        """Faixas de NSU ausentes entre o menor e o maior do ambiente, até ``limite`` faixas, em ordem crescente."""
        ...


def _parametro(nome: str, valor: str | int | datetime) -> bigquery.ScalarQueryParameter:
    if isinstance(valor, datetime):
        return bigquery.ScalarQueryParameter(nome, "TIMESTAMP", valor)
    if isinstance(valor, int):
        return bigquery.ScalarQueryParameter(nome, "INT64", valor)
    return bigquery.ScalarQueryParameter(nome, "STRING", valor)


class BigQueryIndice:
    """Índice no BigQuery.

    :param cliente: Cliente do BigQuery.
    :param projeto: Projeto das tabelas.
    :param dataset: Dataset das tabelas.
    """

    def __init__(self, cliente: bigquery.Client, projeto: str = PROJETO_GCP, dataset: str = DATASET_RAW) -> None:
        self._cliente = cliente
        self._projeto = projeto
        self._dataset = dataset

    def _tabela(self, nome: str) -> str:
        return f"{self._projeto}.{self._dataset}.{nome}"

    def _consultar(self, consulta: str, **parametros: str | int | datetime) -> bigquery.table.RowIterator:
        sql = load_query(QUERIES_ANCHOR, consulta, projeto=self._projeto, dataset=self._dataset)
        configuracao = bigquery.QueryJobConfig(
            query_parameters=[_parametro(nome, valor) for nome, valor in parametros.items()]
        )
        return self._cliente.query(sql, job_config=configuracao).result()

    def _carregar(self, tabela: str, schema: list[bigquery.SchemaField], linhas: Iterable[str]) -> None:
        """Load job de NDJSON com acréscimo (WRITE_APPEND); espera terminar e levanta se falhar."""
        arquivo = io.BytesIO("\n".join(linhas).encode("utf-8"))
        configuracao = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self._cliente.load_table_from_file(arquivo, self._tabela(tabela), job_config=configuracao).result()

    def garantir_tabelas(self) -> None:
        """Cria ``documentos`` (partição e filtro obrigatório, cluster por nsu), o cursor e os inexistentes."""
        documentos = bigquery.Table(self._tabela(TABELA_DOCUMENTOS), schema=SCHEMA_DOCUMENTOS)
        documentos.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="data_hora_geracao"
        )
        documentos.clustering_fields = ["nsu"]
        documentos.require_partition_filter = True
        self._cliente.create_table(documentos, exists_ok=True)
        self._cliente.create_table(bigquery.Table(self._tabela(TABELA_CURSOR), schema=SCHEMA_CURSOR), exists_ok=True)
        self._cliente.create_table(
            bigquery.Table(self._tabela(TABELA_INEXISTENTES), schema=SCHEMA_INEXISTENTES), exists_ok=True
        )

    def ler_cursor(self, ambiente: str) -> int | None:
        """Último NSU gravado do ambiente."""
        linhas = list(self._consultar("ler_cursor", ambiente=ambiente))
        return int(linhas[0]["ultimo_nsu"]) if linhas else None

    def gravar_cursor(self, ambiente: str, ultimo_nsu: int, atualizado_em: datetime) -> None:
        """``MERGE`` que cria ou avança o cursor."""
        self._consultar("gravar_cursor", ambiente=ambiente, ultimo_nsu=ultimo_nsu, atualizado_em=atualizado_em)

    def nsus_indexados(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> set[int]:
        """NSUs já indexados na faixa."""
        linhas = self._consultar("nsus_indexados", ambiente=ambiente, nsu_ini=nsu_ini, nsu_fim=nsu_fim)
        return {int(linha["nsu"]) for linha in linhas}

    def documentos_na_faixa(self, ambiente: str, nsu_ini: int, nsu_fim: int) -> list[DocumentoIndexado]:
        """NSU, hash, origem e posição no raw das linhas indexadas na faixa."""
        linhas = self._consultar("documentos_na_faixa", ambiente=ambiente, nsu_ini=nsu_ini, nsu_fim=nsu_fim)
        return [
            DocumentoIndexado(
                int(linha["nsu"]), linha["xml_sha256"], linha["origem"], linha["gcs_uri"], int(linha["linha"])
            )
            for linha in linhas
        ]

    def carregar_documentos(self, linhas: Sequence[LinhaIndice]) -> None:
        """Load job das linhas de índice."""
        self._carregar(TABELA_DOCUMENTOS, SCHEMA_DOCUMENTOS, (linha.para_json() for linha in linhas))

    def carregar_inexistentes(self, ambiente: str, nsus: Sequence[int], verificado_em: datetime) -> None:
        """Load job dos NSUs confirmados inexistentes."""
        instante = verificado_em.isoformat()
        self._carregar(
            TABELA_INEXISTENTES,
            SCHEMA_INEXISTENTES,
            (json.dumps({"ambiente": ambiente, "nsu": nsu, "verificado_em": instante}) for nsu in nsus),
        )

    def lacunas(self, ambiente: str, limite: int) -> list[FaixaNsu]:
        """Faixas ausentes entre o menor e o maior NSU do ambiente."""
        linhas = self._consultar("lacunas", ambiente=ambiente, limite=limite)
        return [FaixaNsu(int(linha["nsu_ini"]), int(linha["nsu_fim"])) for linha in linhas]
