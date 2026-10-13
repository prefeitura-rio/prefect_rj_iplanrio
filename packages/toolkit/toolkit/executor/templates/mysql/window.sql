{#
{
  "kind": "template",
  "description": "Restrict a remote MySQL query to a date selection.",
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
WHERE DATE({{ column }}) >= {{ lower }}
{%- elif kind == "time_range" %}
WHERE DATE({{ column }}) >= {{ lower }}
    AND DATE({{ column }}) < {{ upper }}
{%- endif %}
