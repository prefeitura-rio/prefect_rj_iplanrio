{#
{
  "kind": "macro",
  "name": "duckdb.text",
  "description": "Remove NUL bytes and replace line breaks with spaces in a text column.",
  "inputs": {
    "column": "SQL-safe column identifier."
  },
  "returns": "A text expression."
}
#}
{% macro text(column) -%}
replace(regexp_replace({{ column }}, '[\r\n]', ' ', 'g'), chr(0), '')
{%- endmacro %}
