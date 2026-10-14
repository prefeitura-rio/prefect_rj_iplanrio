{#
{
  "kind": "template",
  "description": "Describe the columns that a native query returns, without reading its rows.",
  "inputs": {
    "type": "Source name: postgres, mysql, mssql or oracle.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import native_query %}
DESCRIBE SELECT * FROM {{ native_query(type, alias, secret, query) }}
