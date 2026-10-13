{#
{
  "kind": "template",
  "description": "Restrict a remote query to a date selection.",
  "inputs": {
    "query": "The remote SQL query.",
    "kind": "Selection type: all, from_date or time_range.",
    "column": "Plain column identifier.",
    "lower": "Inclusive lower date literal.",
    "upper": "Exclusive upper date literal."
  }
}
#}
SELECT * FROM ({{ query }}) AS src
{%- if kind == "from_date" %}
WHERE CAST({{ column }} AS DATE) >= CAST({{ lower }} AS DATE)
{%- elif kind == "time_range" %}
WHERE CAST({{ column }} AS DATE) >= CAST({{ lower }} AS DATE)
    AND CAST({{ column }} AS DATE) < CAST({{ upper }} AS DATE)
{%- endif %}
