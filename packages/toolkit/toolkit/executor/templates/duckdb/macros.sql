{#
{
  "kind": "macro",
  "name": "duckdb.arcgis_layer",
  "description": "Read an ArcGIS REST layer with ST_Read. The driver pages through the layer.",
  "inputs": {
    "url": "SQL literal with the layer URL, without /query.",
    "where": "SQL literal with the ArcGIS where clause. It is URL-encoded. The default reads every feature."
  },
  "returns": "A table expression for FROM."
}
#}
{% macro arcgis_layer(url, where="'1=1'") -%}
ST_Read('ESRIJSON:' || {{ url }} || '/query?outFields=*&f=json&where=' || url_encode({{ where }}))
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.native_query",
  "description": "Run a query in the dialect of a source database, through the function of its extension.",
  "inputs": {
    "type": "Source name: postgres, mysql, mssql or oracle.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query."
  },
  "returns": "A table expression for FROM."
}
#}
{% macro native_query(type, alias, secret, query) -%}
{%- if type == "postgres" -%}
postgres_query({{ alias }}, {{ query }})
{%- elif type == "mysql" -%}
mysql_query({{ alias }}, {{ query }})
{%- elif type == "mssql" -%}
mssql_scan({{ alias }}, {{ query }})
{%- elif type == "oracle" -%}
oracle_query({{ secret }}, {{ query }})
{%- endif -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.normalized_columns",
  "description": "List the columns of a result, each renamed to a slug that BigQuery accepts.",
  "inputs": {
    "columns": "Each column as a mapping with its source and target identifiers. Build them with column_aliases."
  },
  "returns": "A select list such as \"Nome Completo\" AS \"nome_completo\"."
}
#}
{% macro normalized_columns(columns) -%}
{%- for column in columns -%}
{{ column.source }} AS {{ column.target }}{{ ", " if not loop.last }}
{%- endfor -%}
{%- endmacro %}
