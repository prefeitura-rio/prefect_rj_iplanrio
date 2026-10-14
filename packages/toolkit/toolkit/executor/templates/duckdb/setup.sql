{#
{
  "kind": "template",
  "description": "Set the time zone, the memory limit and the temp directory, and load DuckDB extensions.",
  "inputs": {
    "extensions": "Extensions to install and load, each with name and community.",
    "time_zone": "Time zone literal, so a TIMESTAMPTZ is cut into days the same way on every machine.",
    "memory_limit": "DuckDB memory limit literal, or false to keep the default.",
    "temp_directory": "Directory literal for spilling to disk, or false to keep the default."
  }
}
#}
-- noqa: disable=PRS
SET TimeZone = {{ time_zone }};
{% if memory_limit %}
SET memory_limit = {{ memory_limit }};
{% endif %}
{% if temp_directory %}
SET temp_directory = {{ temp_directory }};
{% endif %}
{% include "duckdb/load_extensions.sql" %}
