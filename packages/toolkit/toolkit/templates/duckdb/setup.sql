{#
{
  "kind": "template",
  "description": "Load DuckDB extensions and create the GCS access secret.",
  "inputs": {
    "extensions": "Extensions to install and load, each with name and community.",
    "has_gcs_secret": "Whether HMAC keys are configured.",
    "gcs_key_id": "GCS HMAC access id literal.",
    "gcs_secret": "GCS HMAC secret literal.",
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
CREATE SECRET (
    TYPE gcs,
    KEY_ID {{ gcs_key_id }},
    SECRET {{ gcs_secret }}
);
{% endif %}
