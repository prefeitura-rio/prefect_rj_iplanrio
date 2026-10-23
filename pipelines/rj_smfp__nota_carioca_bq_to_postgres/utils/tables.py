"""As duas tabelas de cada slot (``exigiveis_a`` e ``exigiveis_b``) e a chave que ordena o checksum delas."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SlotTable:
    """Uma tabela do slot e a tabela do BigQuery (export do ``bq_to_oracle``) que a alimenta.

    :param bq_name: Nome da tabela no manifesto (BigQuery).
    :param pg_name: Nome da tabela em cada schema de slot.
    :param sort_key: Colunas que ordenam as linhas do checksum (a chave natural; o desempate é o hash da linha).
    """

    bq_name: str
    pg_name: str
    sort_key: tuple[str, ...]


SUMMARY = SlotTable(
    bq_name="MVT_NOTAS_NACIONAIS_EXIGIVEIS",
    pg_name="mvt_notas_nacionais_exigiveis",
    sort_key=(
        "data_competencia_municipio",
        "cpf_cnpj_responsavel",
        "tipo_retencao",
        "opcao_simples_nacional",
        "retencao",
    ),
)
DETAIL = SlotTable(
    bq_name="MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES",
    pg_name="mvt_notas_nacionais_exigiveis_detalhes",
    sort_key=("dps", "data_competencia_municipio", "chave_acesso", "nota_nacional"),
)
REQUIRED = (SUMMARY, DETAIL)
"""Tabelas que o export precisa trazer; as demais do manifesto (como os detalhes do DFEN) não vão para o Postgres."""
