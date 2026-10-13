{#
{
  "kind": "template",
  "description": "Restrict a remote Oracle query to a date selection.",
  "inputs": {
    "query": "The remote SQL query.",
    "kind": "Selection type: all, from_date or time_range.",
    "column": "Plain column identifier.",
    "lower": "Inclusive lower date literal.",
    "upper": "Exclusive upper date literal."
  }
}
#}
SELECT * FROM ({{ query }}) src
{%- if kind == "from_date" %}
WHERE {{ column }} >= TO_DATE({{ lower }}, 'YYYY-MM-DD')
{%- elif kind == "time_range" %}
WHERE {{ column }} >= TO_DATE({{ lower }}, 'YYYY-MM-DD')
    AND {{ column }} < TO_DATE({{ upper }}, 'YYYY-MM-DD')
{%- endif %}
