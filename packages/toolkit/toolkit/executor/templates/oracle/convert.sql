{#
{
  "kind": "template",
  "description": "Wrap a query in a select list that reads each column in a form that Arrow and Delta hold.",
  "inputs": {
    "sql": "The query to wrap.",
    "columns": "Each column as a mapping with its identifier in name, and the kind of its expression: plain, utc, number_text, text, rowid or xml."
  }
}
#}
-- noqa: disable=PRS
{% from "oracle/macros.sql" import select_list %}
SELECT {{ select_list(columns) }}
FROM (
{{ sql }}
)
