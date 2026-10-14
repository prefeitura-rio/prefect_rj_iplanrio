{#
{
  "kind": "template",
  "description": "Read the rows of one window of an Oracle table or query, or no row, only the columns.",
  "inputs": {
    "source": "The table as an identifier, or the query in parentheses.",
    "scn": "True to read the table as of the bind :scn. Only for a table.",
    "empty": "True to return no row.",
    "predicates": "The filters and the window, each with a column identifier, an operator and, for most operators, the text of a bind."
  }
}
#}
-- noqa: disable=PRS
{% from "oracle/macros.sql" import where_clause %}
SELECT *
FROM {{ source }}{% if scn %} AS OF SCN :scn{% endif %}
{% if empty %}WHERE 1 = 0{% else %}{{ where_clause(predicates) }}{% endif %}
