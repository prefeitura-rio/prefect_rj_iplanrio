{#
{
  "kind": "template",
  "description": "A native query that runs on the server of a source and gives one row: how many rows a chunk has, and the highest value of one column. A dump runs it with the native query function of the extension, and an index on the column answers it, so no row is read or moved.",
  "inputs": {
    "type": "Dialect: postgres, mysql or mssql.",
    "table": "The table with its quoted names, as the dialect writes them.",
    "column": "The quoted name of the column to take the highest value of.",
    "predicate": "The chunk as a native condition, or an empty text for the whole table."
  }
}
#}
SELECT
    {% if type == "mssql" %}COUNT_BIG(*){% else %}COUNT(*){% endif %} AS n,
    MAX({{ column }}) AS last
FROM {{ table }}
{% if predicate %}WHERE {{ predicate }}{% endif %}
