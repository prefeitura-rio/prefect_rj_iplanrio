{#
{
  "kind": "template",
  "description": "Read the rows of one chunk of an Oracle table or query, or no row, only the columns.",
  "inputs": {
    "source": "The table as an identifier, or the query in parentheses.",
    "scn": "True to read the table as of the bind :scn. Only for a table.",
    "empty": "True to return no row.",
    "names": "The identifiers of the columns to read. Empty means every column. Optional.",
    "filters": "The filters, each the text of one condition in parentheses. Optional.",
    "predicates": "The chunk, each with a column identifier, an operator and, for most operators, the text of a bind. Optional."
  }
}
#}
-- noqa: disable=PRS
{% from "oracle/macros.sql" import where_clause %}
SELECT {% if names | default([]) %}{{ names | join(", ") }}{% else %}*{% endif %}
FROM {{ source }}{% if scn %} AS OF SCN :scn{% endif %}
{% if empty %}WHERE 1 = 0{% else %}{{ where_clause(predicates | default([]), filters | default([])) }}{% endif %}
