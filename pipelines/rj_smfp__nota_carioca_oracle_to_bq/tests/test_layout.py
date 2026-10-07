import pytest
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.bigquery import read_layout
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan, default_layout, resolve_destination
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import (
    BqField,
    LayoutError,
    SchemaChangeError,
    TableLayout,
    TableState,
    assert_layout_fields,
    assert_layouts_match,
)

EXTRACTED_AT = "_airbyte_extracted_at"
FIELDS = (
    BqField(EXTRACTED_AT, "TIMESTAMP", "REQUIRED"),
    BqField("TELEFONE", "NUMERIC"),
    BqField("PESSOA_NACIONAL", "STRING"),
)
COLUMNS = (OracleColumn("TELEFONE", "NUMBER", 20, 0), OracleColumn("PESSOA_NACIONAL", "VARCHAR2", None, None))
AIRBYTE_LAYOUT = TableLayout("DAY", EXTRACTED_AT, (EXTRACTED_AT,))


def plan_for(table_id: str = "PESSOAS_NACIONAIS") -> TablePlan:
    return TablePlan(table_id, "DFEN", COLUMNS, FIELDS, default_layout(table_id), None)


def final_with(layout: TableLayout, fields: tuple[BqField, ...] = FIELDS) -> TableState:
    return TableState(fields=fields, layout=layout, labels={}, num_rows=0)


def test_temp_mirrors_the_final_layout_when_it_differs_from_the_default() -> None:
    resolved = resolve_destination(plan_for(), final_with(AIRBYTE_LAYOUT))

    assert resolved.layout == AIRBYTE_LAYOUT
    assert plan_for().layout == TableLayout("DAY", EXTRACTED_AT, ("PESSOA_NACIONAL", EXTRACTED_AT))
    assert resolved.changes is not None


def test_temp_mirrors_a_non_default_partition_type_and_unpartitioned_final() -> None:
    monthly = TableLayout("MONTH", EXTRACTED_AT, ())
    flat = TableLayout(None, None, ("TELEFONE",))

    assert resolve_destination(plan_for(), final_with(monthly)).layout == monthly
    assert resolve_destination(plan_for(), final_with(flat)).layout == flat


def test_default_layout_is_used_only_when_the_final_table_is_absent() -> None:
    resolved = resolve_destination(plan_for(), None)

    assert resolved.layout == TableLayout("DAY", EXTRACTED_AT, ("PESSOA_NACIONAL", EXTRACTED_AT))
    assert resolved.changes is None


def test_plan_fails_when_a_final_clustering_field_is_missing_from_the_new_schema() -> None:
    final = final_with(TableLayout("DAY", EXTRACTED_AT, ("NAO_EXISTE", EXTRACTED_AT)))

    with pytest.raises(LayoutError, match="NAO_EXISTE"):
        resolve_destination(plan_for(), final)


def test_plan_fails_when_the_final_partition_field_is_missing_from_the_new_schema() -> None:
    with pytest.raises(LayoutError, match="OUTRA_DATA"):
        assert_layout_fields("T", TableLayout("DAY", "OUTRA_DATA", ()), FIELDS)


def test_schema_incompatibility_is_still_reported_before_layout() -> None:
    final = final_with(AIRBYTE_LAYOUT, (*FIELDS, BqField("COLUNA_REMOVIDA", "STRING")))

    with pytest.raises(SchemaChangeError, match="COLUNA_REMOVIDA"):
        resolve_destination(plan_for(), final)


def test_layouts_must_match_exactly_for_the_copy_job() -> None:
    assert_layouts_match("T", AIRBYTE_LAYOUT, AIRBYTE_LAYOUT)
    for other in (
        TableLayout("DAY", EXTRACTED_AT, ("PESSOA_NACIONAL", EXTRACTED_AT)),
        TableLayout("DAY", EXTRACTED_AT, (EXTRACTED_AT, "PESSOA_NACIONAL")),
        TableLayout("MONTH", EXTRACTED_AT, (EXTRACTED_AT,)),
        TableLayout(None, None, (EXTRACTED_AT,)),
    ):
        with pytest.raises(LayoutError, match="incompatível"):
            assert_layouts_match("T", other, AIRBYTE_LAYOUT)


def test_read_layout_reports_time_partition_and_clustering_from_table_metadata() -> None:
    table = bigquery.Table("p.d.t")
    table.time_partitioning = bigquery.TimePartitioning(type_="DAY", field=EXTRACTED_AT)
    table.clustering_fields = [EXTRACTED_AT]

    assert read_layout(table) == AIRBYTE_LAYOUT
    assert read_layout(bigquery.Table("p.d.t")) == TableLayout(None, None, ())


def test_read_layout_refuses_integer_range_partitioning() -> None:
    table = bigquery.Table("p.d.t")
    table.range_partitioning = bigquery.RangePartitioning(field="ID", range_=bigquery.PartitionRange(0, 10, 1))

    with pytest.raises(LayoutError, match="faixa"):
        read_layout(table)
