"""Adaptador do BigQuery: índice, marca, quarentena e cargas por load job (nunca streaming)."""

import io
import re
from collections.abc import Mapping, Sequence

from google.api_core.exceptions import Conflict, GoogleAPICallError
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_parser.constants import (
    QUERIES_ANCHOR,
    TABELA_INDICE,
    TABELA_MARCA,
    TABELA_QUARENTENA,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.entradas import EntradaIndice
from pipelines.rj_smfp__nota_carioca_parser.utils.linhas import serializar_ndjson
from pipelines.rj_smfp__nota_carioca_parser.utils.tabelas import ESPECIFICACOES, schema_bigquery
from prefect_rj_iplanrio.sql import load_query

MAX_TENTATIVAS_LOAD = 5
DESDE_PADRAO = "2000-01-01"
ID_INVALIDO = re.compile(r"[^A-Za-z0-9_-]")


def identificador_do_job(tabela: str, lote_id: str, tentativa: int = 0) -> str:
    """Id do load job: determinístico por tabela e lote, para que repetir o lote não duplique as linhas.

    O BigQuery recusa (409) um id que já existe; um job que falhou deixa o id gasto, então a tentativa seguinte usa
    ``-r<n>``. Só letras, números, ``_`` e ``-``.
    """
    base = ID_INVALIDO.sub("_", f"parser-{tabela}-{lote_id}")[:1000]
    return base if tentativa == 0 else f"{base}-r{tentativa}"


def entrada_do_indice(linha: Mapping[str, object] | bigquery.Row) -> EntradaIndice:
    """Converte uma linha da consulta ao índice."""
    return EntradaIndice(
        nsu=int(linha["nsu"]),
        chave_acesso=linha["chave_acesso"],
        tipo_documento=linha["tipo_documento"],
        tipo_evento=linha["tipo_evento"],
        data_hora_geracao=linha["data_hora_geracao"],
        gcs_uri=linha["gcs_uri"],
        linha=linha["linha"],
        xml_sha256=linha["xml_sha256"],
        origem=linha["origem"],
        ambiente=linha["ambiente"],
    )


class ArmazemBigQuery:
    """Implementa ``ArmazemRaw`` sobre um dataset do BigQuery (as tabelas raw, a marca, a quarentena e o índice)."""

    def __init__(
        self,
        cliente: bigquery.Client,
        projeto: str,
        dataset: str,
        ambiente: str,
        desde: str = DESDE_PADRAO,
    ) -> None:
        self.cliente = cliente
        self.projeto = projeto
        self.dataset = dataset
        self.ambiente = ambiente
        self.desde = desde

    def _sql(self, nome: str) -> str:
        return load_query(
            QUERIES_ANCHOR,
            nome,
            project=self.projeto,
            dataset=self.dataset,
            indice=TABELA_INDICE,
            marca=TABELA_MARCA,
            quarentena=TABELA_QUARENTENA,
        )

    def _consultar(self, nome: str, *parametros: bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter):
        config = bigquery.QueryJobConfig(query_parameters=list(parametros))
        return list(self.cliente.query(self._sql(nome), job_config=config).result())

    def _filtro_do_indice(self) -> list[bigquery.ScalarQueryParameter]:
        return [
            bigquery.ScalarQueryParameter("desde", "STRING", self.desde),
            bigquery.ScalarQueryParameter("ambiente", "STRING", self.ambiente),
        ]

    def ultima_marca(self) -> int:
        return int(self._consultar("ultima_marca")[0]["ultimo_nsu"])

    def listar_indice(self, apos_nsu: int, limite: int) -> list[EntradaIndice]:
        linhas = self._consultar(
            "listar_indice",
            *self._filtro_do_indice(),
            bigquery.ScalarQueryParameter("apos_nsu", "INT64", apos_nsu),
            bigquery.ScalarQueryParameter("limite", "INT64", limite),
        )
        return [entrada_do_indice(linha) for linha in linhas]

    def entradas_por_nsu(self, nsus: Sequence[int]) -> list[EntradaIndice]:
        linhas = self._consultar(
            "entradas_por_nsu", *self._filtro_do_indice(), bigquery.ArrayQueryParameter("nsus", "INT64", list(nsus))
        )
        return [entrada_do_indice(linha) for linha in linhas]

    def buscar_nfse_por_chave(self, chave_acesso: str) -> EntradaIndice | None:
        linhas = self._consultar(
            "buscar_nfse_por_chave",
            *self._filtro_do_indice(),
            bigquery.ScalarQueryParameter("chave_acesso", "STRING", chave_acesso),
        )
        return entrada_do_indice(linhas[0]) if linhas else None

    def listar_quarentena_pendente(self, versao_parser: str, limite: int) -> list[int]:
        linhas = self._consultar(
            "quarentena_pendente",
            bigquery.ScalarQueryParameter("versao_parser", "STRING", versao_parser),
            bigquery.ScalarQueryParameter("limite", "INT64", limite),
        )
        return [int(linha["nsu"]) for linha in linhas]

    def garantir_tabelas(self) -> None:
        """Cria as tabelas raw, a marca e a quarentena (com partição e cluster) se ainda não existirem."""
        for nome, especificacao in ESPECIFICACOES.items():
            tabela = bigquery.Table(
                f"{self.projeto}.{self.dataset}.{nome}",
                schema=[bigquery.SchemaField.from_api_repr(campo) for campo in schema_bigquery(nome)],
            )
            if especificacao.particao:
                tabela.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field=especificacao.particao
                )
            tabela.clustering_fields = list(especificacao.cluster)
            self.cliente.create_table(tabela, exists_ok=True)

    def carregar(self, tabela: str, lote_id: str, linhas: Sequence[Mapping[str, object]]) -> None:
        """Load job NDJSON em modo append; o id determinístico do job torna repetir o lote um no-op.

        :raises google.api_core.exceptions.GoogleAPICallError: Se o load falhar.
        :raises RuntimeError: Se todos os ids de tentativa estiverem gastos por jobs que falharam.
        """
        configuracao = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[bigquery.SchemaField.from_api_repr(campo) for campo in schema_bigquery(tabela)],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        )
        destino = f"{self.projeto}.{self.dataset}.{tabela}"
        corpo = serializar_ndjson(linhas)
        for tentativa in range(MAX_TENTATIVAS_LOAD):
            job_id = identificador_do_job(tabela, lote_id, tentativa)
            try:
                job = self.cliente.load_table_from_file(
                    io.BytesIO(corpo), destino, job_id=job_id, job_config=configuracao, rewind=True
                )
                ja_existia = False
            except Conflict:
                job = self.cliente.get_job(job_id)
                ja_existia = True
            try:
                job.result()
            except GoogleAPICallError:
                if not ja_existia:
                    raise
                continue
            return
        raise RuntimeError(f"Todos os ids de load job de {tabela}/{lote_id} estão gastos por jobs que falharam.")

    def gravar_marca(self, versao_parser: str, ultimo_nsu: int) -> None:
        self.carregar(
            TABELA_MARCA, f"{versao_parser}-{ultimo_nsu}", [{"versao_parser": versao_parser, "ultimo_nsu": ultimo_nsu}]
        )
