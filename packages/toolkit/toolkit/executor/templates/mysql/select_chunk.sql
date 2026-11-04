{#
{
  "kind": "template",
  "description": "Read the rows of one chunk of a MySQL table or query, or no row, only the columns.",
  "inputs": {
    "source": "The table as quoted names, or the query in parentheses with an alias.",
    "empty": "True to return no row.",
    "names": "The quoted names of the columns to read. Empty means every column. Optional.",
    "filters": "The filters, each the text of one condition in parentheses. Optional.",
    "predicates": "The chunk, each the text of one condition with its %s placeholders. Optional."
  }
}
#}
-- noqa: disable=PRS
SELECT {% if names | default([]) %}{{ names | join(", ") }}{% else %}*{% endif %}
FROM {{ source }}
{% if empty %}WHERE 1 = 0{% elif filters | default([]) or predicates | default([]) %}WHERE {{ (filters | default([]) + predicates | default([])) | join(" AND ") }}{% endif %}
