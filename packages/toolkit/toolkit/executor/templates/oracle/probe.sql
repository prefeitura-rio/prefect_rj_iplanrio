{#
{
  "kind": "template",
  "description": "Give one row for a chunk of an Oracle table: how many rows it has, and the highest system change number that wrote one of them. It changes when a row of the chunk is inserted, updated or deleted. ORA_ROWSCN is kept by the block, so a write to a row of the same block moves it too, and that is a false positive and never a miss.",
  "inputs": {
    "source": "The table as an identifier.",
    "scn": "True to read the table as of the bind :scn, the point that the dump reads.",
    "filters": "The filters, each the text of one condition in parentheses. Optional.",
    "predicates": "The chunk, each with a column identifier, an operator and, for most operators, the text of a bind. Optional."
  }
}
#}
-- noqa: disable=PRS
{% from "oracle/macros.sql" import where_clause %}
SELECT COUNT(*) AS "n", COALESCE(MAX(ORA_ROWSCN), 0) AS "total"
FROM {{ source }}{% if scn %} AS OF SCN :scn{% endif %}
{{ where_clause(predicates | default([]), filters | default([])) }}
