# -*- coding: utf-8 -*-
"""
Validação pós-carga: compara contagem de registros entre source e BigQuery.

Valida que o delta entre source e BQ está dentro de uma tolerância aceitável.
Delta > tolerância → RuntimeError que interrompe o flow e aciona retry/alerta.

Usado ao final de cada fase como checkpoint de qualidade.
"""

from __future__ import annotations

from google.cloud import bigquery
from prefect import task


@task(log_prints=True)
def validate_row_count(
    source_count: int,
    project_id: str,
    dataset_id: str,
    table_id: str,
    partition_date: str,
    partition_field: str = "data_particao",
    tolerance_pct: float = 0.01,
    write_mode: str = "append",
) -> bool:
    """
    Verifica se a contagem de linhas no BigQuery corresponde à contagem no source.

    Para write_mode='replace': valida que BQ == source (dentro da tolerância).
    Para write_mode='append' ou 'merge': valida que BQ >= source (acúmulo esperado).

    Args:
        source_count    : Total de registros extraídos do Salesforce.
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset de destino.
        table_id        : Tabela de destino.
        partition_date  : Data da partição no formato 'YYYY-MM-DD'.
        partition_field : Campo de partição. Padrão: 'data_particao'.
        tolerance_pct   : Tolerância máxima de delta (0.01 = 1%). Padrão: 1%.
                          Usado apenas para write_mode='replace'.
        write_mode      : 'append', 'replace' ou 'merge'. Padrão: 'append'.

    Returns:
        True se validação passar.

    Raises:
        RuntimeError: Se validação falhar.
    """
    if source_count == 0:
        print(f"[VALIDATE] '{table_id}': source vazio — validação pulada.")
        return True

    client = bigquery.Client(project=project_id)
    full_id = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{full_id}`
        WHERE {partition_field} = '{partition_date}'
    """

    print(f"[VALIDATE] Contando linhas em '{table_id}' para partição '{partition_date}'...")
    result = client.query(query).result()
    bq_count = next(iter(result)).cnt

    print(
        f"[VALIDATE] '{table_id}': source={source_count}, BQ={bq_count}, "
        f"write_mode={write_mode}"
    )

    if write_mode == "replace":
        delta = abs(source_count - bq_count)
        delta_pct = delta / source_count if source_count > 0 else 0.0
        if delta_pct > tolerance_pct:
            raise RuntimeError(
                f"[VALIDATE] FAIL '{table_id}': delta {delta_pct:.2%} excede tolerância "
                f"{tolerance_pct:.2%}. source={source_count}, BQ={bq_count}."
            )
    else:
        # append/merge: BQ deve ter pelo menos os registros do source
        if bq_count < source_count:
            raise RuntimeError(
                f"[VALIDATE] FAIL '{table_id}': BQ ({bq_count}) < source ({source_count}). "
                f"Registros podem não ter sido inseridos."
            )

    print(f"[VALIDATE] '{table_id}': OK.")
    return True
