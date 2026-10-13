{#
{
  "kind": "template",
  "description": "Copy a source to Parquet or CSV in the staging bucket, with normalized columns, hive date partitions and an optional ingestion timestamp.",
  "inputs": {
    "source": "DuckDB FROM expression.",
    "columns": "Projection items, each with source, alias and clean.",
    "date_column": "Normalized date column identifier, or false when not partitioned.",
    "year_column": "Year partition identifier.",
    "month_column": "Month partition identifier.",
    "day_column": "Date partition identifier.",
    "add_timestamp": "Whether to add the ingestion timestamp column.",
    "timestamp_column": "Ingestion timestamp identifier.",
    "path": "Destination path literal.",
    "format": "FORMAT options: PARQUET or CSV, HEADER.",
    "partition_columns": "Partition identifiers joined by commas, or false.",
    "pattern": "File name pattern literal for partitioned output."
  }
}
#}
{% from "duckdb/macros.sql" import text %}
-- noqa: disable=PRS
COPY (
    WITH renamed AS (
        SELECT
        {%- for column in columns %}
            {% if column.clean %}{{ text(column.source) }}{% else %}{{ column.source }}{% endif %} AS {{ column.alias }}{{ "," if not loop.last }}
        {%- endfor %}
        FROM {{ source }}
    )
    {%- if date_column %},
    dated AS (
        SELECT *, TRY_CAST({{ date_column }} AS TIMESTAMP) AS date_value FROM renamed
    )
    {%- endif %}
    SELECT
        {% if date_column %}* EXCLUDE (date_value){% else %}*{% endif %}
        {%- if add_timestamp %},
        current_timestamp AS {{ timestamp_column }}
        {%- endif %}
        {%- if date_column %},
        CAST(year(date_value) AS VARCHAR) AS {{ year_column }},
        CAST(month(date_value) AS VARCHAR) AS {{ month_column }},
        CAST(date_value AS DATE) AS {{ day_column }}
        {%- endif %}
    FROM {% if date_column %}dated{% else %}renamed{% endif %}
) TO {{ path }} (
    FORMAT {{ format }}
    {%- if partition_columns %},
    PARTITION_BY ({{ partition_columns }}),
    FILENAME_PATTERN {{ pattern }},
    OVERWRITE_OR_IGNORE
    {%- endif %}
)
