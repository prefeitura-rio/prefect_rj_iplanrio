{#
{
  "kind": "template",
  "description": "Describe the columns that a dump reads, without reading its rows.",
  "inputs": {
    "type": "Source name: postgres, mssql, bigquery, arcgis, gsheets, parquet, csv or json.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query, the layer URL, the spreadsheet id or the file path or glob.",
    "where": "SQL literal with the ArcGIS where clause. Optional, only for arcgis.",
    "srid": "SQL literal with the EPSG code of an ArcGIS answer. Optional, only for arcgis.",
    "sheet": "SQL literal with the sheet name. Optional, only for gsheets.",
    "table": "Identifier of a table of the attached source. Optional. It wins over the query."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import source_table %}
DESCRIBE SELECT * FROM {{ source_table(type, alias, secret, query | default(none), where | default("'1=1'"), sheet | default(none), table | default(none), srid | default(none)) }}
