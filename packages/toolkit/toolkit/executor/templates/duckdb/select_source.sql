{#
{
  "kind": "template",
  "description": "Read every row of a source, to stream it in batches.",
  "inputs": {
    "type": "Source name: postgres, mysql, mssql, oracle or bigquery.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query. Optional when there is a table.",
    "table": "Identifier of a table of the attached source. Optional. It wins over the query."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import source_table %}
SELECT * FROM {{ source_table(type, alias, secret, query | default(none), "'1=1'", none, table | default(none)) }}
