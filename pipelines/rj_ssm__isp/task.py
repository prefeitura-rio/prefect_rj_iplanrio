"""Tasks do pipeline rj_ssm__isp."""

import shutil
import uuid
from pathlib import Path
from typing import Optional, Literal
from datetime import datetime, timedelta, timezone
import pandas as pd
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.pandas import parse_date_columns, to_partitions
from prefect import task
from iplanrio.pipelines_utils.logging import log
from tenacity import retry

from client import IspGeoClient
from constants import (
    DEFAULT_CRIME_TITLES,
    MAX_CONCURRENT_REQUESTS,
    MUNICIPIO_RIO_DE_JANEIRO,
    TMP_BASE,
    SP_TZ,
    FASE_LABEL
)
from utils import build_dataframe, build_where, decode_row, resolve_crime_codes, _ultimo_trimestre, _add_id_hash, resolve_dates as _resolve_dates




@task(retry_delay_seconds=5, retries=3)
def fetch_ocorrencias_task(
    data_inicio: str,
    data_fim: str,
    todos: bool = False,
    municipio: int = MUNICIPIO_RIO_DE_JANEIRO,
    max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
    fase: Literal["consolidados", "errata", "parcial"] = "parcial",
) -> pd.DataFrame:
    """Extrai ocorrências do ISP-GEO para o período e filtro informados.

    ``todos=True`` traz todos os registros do período/município, ignorando o
    filtro de tipo de delito — ver nota em :func:`pipelines.rj_ssm__isp.utils.build_where`
    sobre códigos de ``delito_do`` órfãos no domínio da camada.

    :param data_inicio: Data de início (``YYYY-MM-DD``), inclusive.
    :param data_fim: Data de fim (``YYYY-MM-DD``), inclusive.
    :param todos: Se ``True``, ignora o filtro de tipo de delito.
    :param municipio: Código IBGE do município do fato.
    :param max_concurrent_requests: Número máximo de requisições assíncronas simultâneas.
    :param fase: Fase de disponibilidade dos dados.
    :returns: DataFrame com uma linha por ocorrência, colunas nomeadas para o BigQuery.
    """
    with IspGeoClient() as client:
        log("Baixando domínios da camada...", level='info')
        domains = client.get_field_domains()

        if todos:
            crime_codes = None
            log(
                "Tipos de DO: SEM filtro (todos os registros do período/município).",
                level='info'
            )
        else:
            crime_codes = resolve_crime_codes(
                DEFAULT_CRIME_TITLES, domains["delito_do"]
            )
            log(f"Tipos de DO incluídos na consulta: {len(crime_codes)}")

        where = build_where(
            data_inicio=data_inicio,
            data_fim=data_fim,
            crime_codes=crime_codes,
            municipio_cod=municipio,
        )
        log(f"Where: {where}")

        total_no_servidor = client.count_records(where=where)
        log(f"Registros no servidor (returnCountOnly): {total_no_servidor}")
        raw_rows = client.fetch_features(where=where, max_concurrent_requests=max_concurrent_requests)
        log(f"{len(raw_rows)} ocorrências baixadas.")

        if len(raw_rows) != total_no_servidor:
            log(
                f"Divergência entre contagem do servidor {len(raw_rows)} e registros baixados {len(raw_rows)}.",
            )

        codigos_nao_mapeados = sorted(
            {
                r["delito_do"]
                for r in raw_rows
                if r.get("delito_do") not in domains["delito_do"]
            }
        )
        if codigos_nao_mapeados:
            log(
                f"{len(codigos_nao_mapeados)} código(s) de delito_do sem nome no domínio da camada: {', '.join(str(c) for c in codigos_nao_mapeados)}",
            )

        fase_label = FASE_LABEL[fase]
        for row in raw_rows:
            row["fase"] = fase_label
        _add_id_hash(raw_rows)
        rows = [decode_row(r, domains) for r in raw_rows]
    return build_dataframe(rows)


@task
def resolve_dates_task(
    fase: Literal["parcial", "consolidados", "errata"],
    data_inicio: Optional[str],
    data_fim: Optional[str],
) -> dict[str, str]:
    """Wrapper ``@task`` sobre :func:`utils.resolve_dates`.

    Retorna dict em vez de tupla — o Prefect não suporta unpacking direto
    de tuplas retornadas por tasks. Extraia as chaves no flow:

        dates = resolve_dates_task(fase, data_inicio, data_fim)
        data_inicio = dates["data_inicio"]
        data_fim = dates["data_fim"]
    """
    return _resolve_dates(fase, data_inicio, data_fim)


@task
def upload_ocorrencias_task(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> Optional[str]:
    """Particiona por data do fato e envia o DataFrame para o BigQuery via GCS.

    :param dataframe: DataFrame retornado por :func:`fetch_ocorrencias_task`.
    :param dataset_id: Dataset do BigQuery.
    :param table_id: Tabela do BigQuery.
    :param dump_mode: ``"append"`` para acumular dados, ``"overwrite"`` para substituir.
    :returns: Caminho local dos dados enviados, ou ``None`` se não houve dados.
    """
    if dataframe.empty:
        log("Nenhuma ocorrência para enviar — pulando upload.", level='warning')
        return None

    df = dataframe.astype("string")
    df['update_at'] = datetime.now(tz=SP_TZ).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    df, _ = parse_date_columns(
        dataframe=df, partition_date_column="data_fato"
    )

    savepath = f"{TMP_BASE}/{table_id}"
    Path(savepath).mkdir(parents=True, exist_ok=True)

    to_partitions(
        data=df,
        partition_columns=["ano_particao", "mes_particao", "data_particao", "fase"],
        savepath=savepath,
        data_type="parquet",
    )

    log(f"{len(df)} linha(s), {len(df.columns)} coluna(s) → enviando...")

    create_table_and_upload_to_gcs(
        data_path=savepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        source_format="parquet",
        only_staging_dataset=True,
        project_id="rj-ssm",
    )

    shutil.rmtree(savepath, ignore_errors=True)
    log("Upload concluído.", level='info')
    return savepath
