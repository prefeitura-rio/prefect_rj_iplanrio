"""Schema do Parquet gravado no GCS.

Espelha apenas ``parquet_schema`` do módulo de mesmo nome do PR #394 (``rj_smfp__nota_carioca_oracle_to_bq``);
manter em sincronia e remover quando o PR for mesclado.
"""

import pyarrow as pa

from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import (
    AIRBYTE_EXTRACTED_AT,
    AIRBYTE_GENERATION_ID,
    AIRBYTE_RAW_ID,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.columns import OracleColumn, output_type


def parquet_schema(columns: tuple[OracleColumn, ...]) -> pa.Schema:
    """Monta o schema do Parquet: colunas ``_airbyte_*`` (exceto ``_airbyte_meta``) e as do Oracle.

    As colunas ``REQUIRED`` são não nulas no Parquet, senão o load na tabela
    existente falha por mudança de modo.

    :param columns: Colunas da tabela de origem.
    :returns: Schema Arrow.
    """
    return pa.schema(
        [
            pa.field(AIRBYTE_RAW_ID, pa.string(), nullable=False),
            pa.field(AIRBYTE_EXTRACTED_AT, pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field(AIRBYTE_GENERATION_ID, pa.int64()),
            *[pa.field(column.name, output_type(column)) for column in columns],
        ]
    )
