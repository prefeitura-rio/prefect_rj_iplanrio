{#
{
  "kind": "template",
  "description": "Copy one chunk of a source to a single local Parquet file, even when it has no rows, so the file keeps the schema.",
  "inputs": {
    "type": "Source name: postgres, mysql, mssql, bigquery, arcgis, gsheets, parquet, csv or json.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query, the layer URL, the spreadsheet id or the file path or glob. Optional when there is a table.",
    "where": "SQL literal with the ArcGIS where clause. Optional, only for arcgis.",
    "srid": "SQL literal with the EPSG code of an ArcGIS answer. Optional, only for arcgis.",
    "sheet": "SQL literal with the sheet name. Optional, only for gsheets.",
    "table": "Identifier of a table of the attached source. Optional. It wins over the query.",
    "filters": "The filters, each a condition in parentheses, built with filter_conditions. Optional.",
    "predicates": "The chunk, built with chunk_predicates. Optional.",
    "columns": "The select list of the result columns, built with select_columns. Empty means every column.",
    "target": "SQL literal with the path of the Parquet file."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import select_columns, source_table, where_clause %}
COPY (
    SELECT {{ select_columns(columns) if columns else "*" }}
    FROM {{ source_table(type, alias, secret, query | default(none), where | default("'1=1'"), sheet | default(none), table | default(none), srid | default(none)) }}
    {{ where_clause(predicates | default([]), filters | default([])) }}
) TO {{ target }} (FORMAT PARQUET)
