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
