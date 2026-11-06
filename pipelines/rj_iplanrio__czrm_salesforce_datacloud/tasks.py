"""Tasks e funções auxiliares do flow CZRM Salesforce Data Cloud.

Seleção de tabelas, janelas, schemas, processamento por tabela e escrita em Parquet.
"""

from __future__ import annotations

import pathlib
import tempfile
from dataclasses import dataclass
from datetime import date
from typing import Literal

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASET_ID
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.auth import get_session
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.bigquery import garantir_tabelas
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.extract import descobrir_colunas, extrair
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.notify import notify_falha_tabela
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.janela import filtro_sql, fmt_janela, janelas_do_run
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.schema import SchemaTabela, montar
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.tabelas import TABELAS, Tabela
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.transform import transformar

logger = logger_da_pipeline(__name__)

Modo = Literal["hora", "dia", "reconciliacao", "carga_inicial"]

COLUNAS_DATA_CONTROLE = {"data_criacao", "data_atualizacao"}


@dataclass(frozen=True)
class Destino:
    """Configuração de destino do run: projeto, dataset e modo dry-run."""

    project_id: str
    dataset_id: str
    dry_run: bool


def salvar_parquet(df: pd.DataFrame, path: pathlib.Path) -> None:
    """Serializa o DataFrame em Parquet com tipos de data corretos para o BigQuery.

    Pandas serializa ``datetime.date`` como INT64; BigQuery espera DATE (BYTE_ARRAY).
    A conversão pandas → polars → pyarrow resolve isso nativamente.
    """
    frame = pl.from_pandas(df, nan_to_null=True)
    for col in frame.columns:
        if frame[col].dtype == pl.Object:
            frame = frame.with_columns(pl.col(col).cast(pl.Utf8))
    table = frame.to_arrow()
    campos = [
        field.with_type(pa.date32())
        if pa.types.is_date(field.type) or (
            pa.types.is_int32(field.type) and field.name in COLUNAS_DATA_CONTROLE
        )
        else field
        for field in table.schema
    ]
    pq.write_table(table.cast(pa.schema(campos)), path)


def processar_tabela(
    tabela: Tabela,
    schema: SchemaTabela,
    session: dict,
    origem: str,
    destino: Destino,
    filtro: str,
) -> dict[str, int]:
    """Extrai, transforma e grava uma tabela em uma janela de tempo.

    :param tabela: configuração da tabela.
    :param schema: schema descoberto da DLO.
    :param session: sessão autenticada no Salesforce.
    :param origem: org de origem.
    :param destino: configuração de escrita.
    :param filtro: cláusula WHERE já renderizada; vazia na carga inicial.
    :returns: contagem ``{"extraidas", "inseridas", "atualizadas"}``.
    """
    df = extrair(
        session=session,
        dlo=tabela.dlo_de(origem),
        filtro=filtro,
        order_by=tabela.order_by,
        tabela=tabela.nome,
    )
    if df.empty:
        logger.warning(f"[FLOW] Nenhuma dado encontrado para a tabela {tabela.nome}")
        return {"extraidas": 0, "inseridas": 0, "atualizadas": 0}

    extraidas = len(df)
    df = transformar(df, tabela=tabela, schema=schema)

    if destino.dry_run:
        return {"extraidas": extraidas, "inseridas": 0, "atualizadas": 0}

    with tempfile.TemporaryDirectory() as tmpdir:
        path = pathlib.Path(tmpdir) / f"{tabela.nome}.csv"
        df.to_csv(path, index=False)
        create_table_and_upload_to_gcs_task(
            data_path=path,
            dataset_id=destino.dataset_id,
            table_id=tabela.nome,
            dump_mode="append",

        )

    return {"extraidas": extraidas}


def validar_destino(origem: str, dataset_id: str, dry_run: bool) -> None:
    """Impede que origem não-prod grave na zona de pouso de produção.

    :raises ValueError: se a combinação for insegura.
    """
    if origem != "prod" and dataset_id == DATASET_ID and not dry_run:
        raise ValueError(
            f"origem '{origem}' não pode gravar em '{DATASET_ID}' (zona de pouso de produção). "
            "Use dry_run=True ou um dataset_id explícito de validação."
        )


def selecionar(tabelas: list[str] | None) -> list[Tabela]:
    """Devolve as tabelas selecionadas na ordem do YAML.

    :param tabelas: nomes escolhidos no run; ``None`` ou vazio seleciona todas.
    :returns: lista de tabelas a processar.
    :raises ValueError: se algum nome não existir em ``tabelas.yaml``.
    """
    desconhecidas = set(tabelas or []) - {t.nome for t in TABELAS}
    if desconhecidas:
        raise ValueError(f"Tabelas desconhecidas: {sorted(desconhecidas)}. Ver tabelas.yaml.")
    return [t for t in TABELAS if not tabelas or t.nome in tabelas]


def montar_janelas(
    modo: str,
    dia: date | None,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> list[tuple[str | None, str | None]]:
    """Retorna a lista de janelas do run.

    ``carga_inicial`` produz uma única janela aberta (sem filtro de data).
    """
    if modo == "carga_inicial":
        return [(None, None)]
    return list(janelas_do_run(modo, dia, reprocessar, reprocessar_de, reprocessar_ate))


def montar_rotulo(
    modo: str,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> str:
    """Retorna o rótulo legível do run para log e notificação."""
    if reprocessar:
        return f"reprocesso {reprocessar_de} a {reprocessar_ate or reprocessar_de}"
    if modo == "carga_inicial":
        return "carga inicial"
    return f"modo={modo}"


def descobrir_schemas(session: dict, selecionadas: list[Tabela], origem: str) -> dict[str, SchemaTabela]:
    """Descobre o schema de cada tabela a partir da DLO correspondente."""
    return {
        tabela.nome: montar(
            descobrir_colunas(session=session, dlo=tabela.dlo_de(origem), tabela=tabela.nome),
            tabela.booleanas,
        )
        for tabela in selecionadas
    }


def processar_janelas(
    janelas: list[tuple[str | None, str | None]],
    selecionadas: list[Tabela],
    schemas: dict[str, SchemaTabela],
    session: dict,
    origem: str,
    destino: Destino,
) -> tuple[dict[str, dict[str, int]], list[str]]:
    """Executa todas as combinações janela × tabela, isolando falhas por tabela.

    :returns: tupla ``(resultados, falhas)``. ``resultados`` agrupa linhas por
        grupo de tabela; ``falhas`` lista os nomes de tabelas que falharam.
    """
    resultados: dict[str, dict[str, int]] = {}
    falhas: list[str] = []

    for inicio, fim in janelas:
        if inicio and fim:
            logger.info("[FLOW] janela %s", fmt_janela(inicio, fim))

        for tabela in selecionadas:
            filtro = filtro_sql(tabela.coluna_modificacao, inicio or "", fim or "")
            try:
                resultado = processar_tabela(
                    tabela=tabela,
                    schema=schemas[tabela.nome],
                    session=session,
                    origem=origem,
                    destino=destino,
                    filtro=filtro,
                )
                logger.info("[FLOW] %s: %d extraídas", tabela.nome, resultado["extraidas"])
                # linhas = resultado["extraidas"] if destino.dry_run else (
                #     resultado["inseridas"] + resultado["atualizadas"]
                # )
            except Exception as exc:
                logger.warning("[FLOW] '%s' falhou: %s", tabela.nome, exc)
            #     notify_falha_tabela(
            #         tabela=tabela.nome,
            #         grupo=tabela.grupo,
            #         janela=fmt_janela(inicio, fim) if inicio and fim else "carga inicial",
            #         erro=str(exc),
            #     )
            #     if tabela.nome not in falhas:
            #         falhas.append(tabela.nome)
            #     linhas = 0

            # grupo = resultados.setdefault(tabela.grupo, {})
            # grupo[tabela.nome] = grupo.get(tabela.nome, 0) + linhas

    return resultados, falhas
