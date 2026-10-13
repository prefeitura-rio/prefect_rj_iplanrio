{#
{
  "kind": "template",
  "description": "Restrict a BigQuery query to a date selection.",
  "inputs": {
    "query": "The BigQuery SQL query.",
    "kind": "Selection type: all, from_date or time_range.",
    "column": "Plain column identifier.",
    "lower": "Inclusive lower date literal.",
    "upper": "Exclusive upper date literal."
  }
}
#}
SELECT * FROM ({{ query }}) AS src
{%- if kind == "from_date" %}
WHERE CAST({{ column }} AS DATE) >= DATE({{ lower }})
{%- elif kind == "time_range" %}
WHERE CAST({{ column }} AS DATE) >= DATE({{ lower }})
    AND CAST({{ column }} AS DATE) < DATE({{ upper }})
{%- endif %}
