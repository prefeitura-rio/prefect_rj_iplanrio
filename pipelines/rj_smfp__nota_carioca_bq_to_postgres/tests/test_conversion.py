"""Plano de conversão do CSV do BigQuery para as colunas do slot (as regras do control file do sqlldr)."""

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.conversion import PgColumn, build_plan
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.manifest import BqField


def pg(name: str, data_type: str, *, nullable: bool = True) -> PgColumn:
    return PgColumn(name=name, data_type=data_type, nullable=nullable)


def bq(name: str, field_type: str) -> BqField:
    return BqField(name=name, type=field_type, mode="NULLABLE")


def test_a_column_of_the_slot_missing_from_the_export_is_refused_by_name():
    with pytest.raises(ValueError, match="valor_issqn"):
        build_plan([pg("cpf", "character varying"), pg("valor_issqn", "numeric")], [bq("cpf", "STRING")])


@pytest.mark.parametrize(
    ("column", "field"),
    [
        (pg("valor", "numeric"), bq("valor", "STRING")),
        (pg("nome", "character varying"), bq("nome", "INTEGER")),
        (pg("quando", "timestamp without time zone"), bq("quando", "TIMESTAMP")),
        (pg("bytes", "bytea"), bq("bytes", "STRING")),
    ],
)
def test_a_type_pair_the_sqlldr_does_not_support_is_refused(column: PgColumn, field: BqField):
    with pytest.raises(NotImplementedError, match=column.name):
        build_plan([column], [field])


def test_export_columns_the_slot_does_not_have_are_ignored_and_reported():
    plan = build_plan(
        [pg("cpf", "character varying")],
        [bq("cpf", "STRING"), bq("_bigquery_uid", "STRING"), bq("_bigquery_particao_data", "DATE")],
    )

    assert plan.ignored == ("_bigquery_uid", "_bigquery_particao_data")


def test_columns_are_matched_regardless_of_case():
    plan = build_plan([pg("cpf", "character varying")], [bq("CPF", "STRING")])

    assert plan.ignored == ()
