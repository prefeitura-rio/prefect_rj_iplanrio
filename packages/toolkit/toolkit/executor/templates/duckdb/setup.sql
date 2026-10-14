{#
{
  "kind": "template",
  "description": "Load DuckDB extensions and the GCS filesystem.",
  "inputs": {
    "extensions": "Extensions to install and load, each with name and community.",
    "has_gcs_secret": "Whether HMAC keys are configured, so httpfs is installed.",
    "memory_limit": "DuckDB memory limit literal, or false to keep the default.",
    "temp_directory": "Directory literal for spilling to disk, or false to keep the default."
  }
}
#}
-- noqa: disable=PRS
{% if memory_limit %}
SET memory_limit = {{ memory_limit }};
{% endif %}
{% if temp_directory %}
SET temp_directory = {{ temp_directory }};
{% endif %}
{% include "duckdb/load_extensions.sql" %}
{% if has_gcs_secret %}
INSTALL httpfs;
LOAD httpfs;
{% endif %}
