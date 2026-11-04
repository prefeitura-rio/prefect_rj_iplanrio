{#
{
  "kind": "macro",
  "name": "duckdb.arcgis_layer",
  "description": "Read an ArcGIS REST layer with ST_Read. The driver pages through the layer.",
  "inputs": {
    "url": "SQL literal with the layer URL, without /query.",
    "where": "SQL literal with the ArcGIS where clause. It is URL-encoded. The default reads every feature.",
    "srid": "SQL literal with the EPSG code the server answers in, such as '31983', or none for the layer own system."
  },
  "returns": "A table expression for FROM."
}
#}
{% macro arcgis_layer(url, where="'1=1'", srid=none) -%}
ST_Read('ESRIJSON:' || {{ url }} || '/query?outFields=*&f=json&where=' || url_encode({{ where }}){% if srid %} || '&outSR=' || {{ srid }}{% endif %})
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.native_query",
  "description": "Run a query in the dialect of a source database, through the function of its extension.",
  "inputs": {
    "type": "Source name: postgres, mssql or bigquery.",
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
{%- elif type == "mssql" -%}
mssql_scan({{ alias }}, {{ query }})
{%- elif type == "bigquery" -%}
bigquery_query({{ alias }}, {{ query }})
{%- endif -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.source_table",
  "description": "Read what a dump copies: a native query, an ArcGIS layer, a Google Sheet or local files.",
  "inputs": {
    "type": "Source name: postgres, mssql, bigquery, arcgis, gsheets, parquet, csv or json.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query, the layer URL, the spreadsheet id or the file path or glob.",
    "where": "SQL literal with the ArcGIS where clause. Only for arcgis.",
    "srid": "SQL literal with the EPSG code of an ArcGIS answer, or none. Only for arcgis.",
    "sheet": "SQL literal with the sheet name, or none for the first sheet. Only for gsheets.",
    "table": "Identifier of a table of the attached source, such as src.public.orders, or none. It wins over the query. DuckDB pushes the filters and the columns down to the source."
  },
  "returns": "A table expression for FROM."
}
#}
{% macro source_table(type, alias, secret, query, where="'1=1'", sheet=none, table=none, srid=none) -%}
{%- if table -%}
{{ table }}
{%- elif type == "arcgis" -%}
{{ arcgis_layer(query, where, srid) }}
{%- elif type == "gsheets" -%}
read_gsheet({{ query }}{% if sheet %}, sheet = {{ sheet }}{% endif %})
{%- elif type == "parquet" -%}
read_parquet({{ query }}, hive_partitioning = false)
{%- elif type == "csv" -%}
read_csv({{ query }}, header = true, all_varchar = true, hive_partitioning = false)
{%- elif type == "json" -%}
read_json({{ query }}, hive_partitioning = false)
{%- else -%}
{{ native_query(type, alias, secret, query) }}
{%- endif -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.where_clause",
  "description": "List the filters and the chunk of a dump as a WHERE clause, or nothing when there are none.",
  "inputs": {
    "predicates": "Each condition of the chunk as a mapping with a column identifier, an operator from the fixed list and, for most operators, a value. Build them with chunk_predicates.",
    "filters": "Each filter as the text of one condition, already in parentheses. Build them with filter_conditions. Optional."
  },
  "returns": "A clause such as WHERE (status <> 'x') AND \"dia\" < '2024-01-01'."
}
#}
{% macro where_clause(predicates, filters=[]) -%}
{%- if predicates or filters -%}
WHERE {{ filters | join(" AND ") }}{{ " AND " if filters and predicates }}{% for item in predicates -%}
{{ item.column }} {{ item.op }}{% if item.value is defined %} {{ item.value }}{% endif %}{{ " AND " if not loop.last }}
{%- endfor -%}
{%- endif -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.decoded",
  "description": "Read a source column through a decoder function, or as it is when there is none.",
  "inputs": {
    "source": "SQL of the source column.",
    "decoder": "Name of a function that reads the column, or none."
  },
  "returns": "A SQL expression."
}
#}
{% macro decoded(source, decoder) -%}
{%- if decoder -%}{{ decoder }}({{ source }}){%- else -%}{{ source }}{%- endif -%}
{%- endmacro %}

{#
{
  "kind": "macro",
  "name": "duckdb.select_columns",
  "description": "List the columns of a result, each renamed and, if it has a function, computed by it.",
  "inputs": {
    "columns": "Each column as a mapping with its source and target identifiers, and an optional function identifier or cast type. Build them with select_columns.",
    "decoder": "Name of a function that reads each source column before anything else, or none."
  },
  "returns": "A select list such as clean_text(\"Nome Completo\") AS \"nome_completo\"."
}
#}
{% macro select_columns(columns, decoder=none) -%}
{%- for column in columns -%}
{%- if column.function is defined -%}
{{ column.function }}({{ decoded(column.source, decoder) }})
{%- elif column.cast is defined -%}
CAST({{ decoded(column.source, decoder) }} AS {{ column.cast }})
{%- else -%}
{{ decoded(column.source, decoder) }}
{%- endif %} AS {{ column.target }}{{ ", " if not loop.last }}
{%- endfor -%}
{%- endmacro %}
