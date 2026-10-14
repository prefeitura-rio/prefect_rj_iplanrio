{#
{
  "kind": "template",
  "description": "Copy a native query of a source database to Parquet files, without loading the rows into Python.",
  "inputs": {
    "type": "Source name: postgres, mysql, mssql or oracle.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the native query.",
    "target": "SQL literal with the directory, or the gs:// prefix, of the files.",
    "columns": "The result columns to rename, built with column_aliases, or false to keep their names.",
    "partition_by": "Identifiers of the output columns that name the Hive folders."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import native_query, normalized_columns %}
COPY (
    SELECT {{ normalized_columns(columns) if columns else "*" }}
    FROM {{ native_query(type, alias, secret, query) }}
) TO {{ target }} (
    FORMAT PARQUET,
    FILENAME_PATTERN 'part-{uuid}',
    OVERWRITE_OR_IGNORE true,
    {% if partition_by -%}
    PARTITION_BY ({{ partition_by | join(", ") }})
    {%- else -%}
    FILE_SIZE_BYTES '256MB'
    {%- endif %}
)
