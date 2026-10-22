{#
{
  "kind": "macro",
  "name": "oracle.select_list",
  "description": "List the columns of a read, each as an expression that Arrow and Delta hold.",
  "inputs": {
    "columns": "Each column as a mapping with its identifier in name and the kind of its expression: plain keeps the column, utc gives the instant of a time stamp with a time zone, number_text gives a number as exact text, text and rowid give text, xml gives the document as text."
  },
  "returns": "A select list such as TO_CHAR(\"A\") AS \"A\"."
}
#}
{% macro select_list(columns) -%}
{%- for column in columns -%}
{%- if column.expression == "utc" -%}
SYS_EXTRACT_UTC(CAST({{ column.name }} AS TIMESTAMP WITH TIME ZONE))
{%- elif column.expression == "number_text" -%}
TO_CHAR({{ column.name }}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')
{%- elif column.expression == "text" -%}
TO_CHAR({{ column.name }})
{%- elif column.expression == "rowid" -%}
ROWIDTOCHAR({{ column.name }})
{%- elif column.expression == "xml" -%}
XMLSERIALIZE(CONTENT {{ column.name }} AS CLOB)
{%- else -%}
{{ column.name }}
{%- endif %} AS {{ column.name }}{{ ", " if not loop.last }}
{%- endfor -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "oracle.where_clause",
  "description": "List the filters and the chunk of a read as a WHERE clause with binds, or nothing when there are none.",
  "inputs": {
    "predicates": "Each condition of the chunk as a mapping with a column identifier, an operator from the fixed list and, for most operators, the text of a bind.",
    "filters": "Each filter as the text of one Oracle condition, already in parentheses. Optional."
  },
  "returns": "A clause such as WHERE (\"STATUS\" <> 'x') AND \"ID\" >= :p0."
}
#}
{% macro where_clause(predicates, filters=[]) -%}
{%- if predicates or filters -%}
WHERE {{ filters | join(" AND ") }}{{ " AND " if filters and predicates }}{% for item in predicates -%}
{{ item.column }} {{ item.op }}{% if item.value is defined %} {{ item.value }}{% endif %}{{ " AND " if not loop.last }}
{%- endfor -%}
{%- endif -%}
{%- endmacro %}
