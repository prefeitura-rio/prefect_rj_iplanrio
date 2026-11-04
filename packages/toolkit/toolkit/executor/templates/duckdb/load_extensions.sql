{#
{
  "kind": "template",
  "description": "Install and load DuckDB extensions.",
  "inputs": {
    "extensions": "Extensions to install and load, each with name and community."
  }
}
#}
-- noqa: disable=PRS
{% for extension in extensions %}
INSTALL {{ extension.name }}{% if extension.community %} FROM community{% endif %};
LOAD {{ extension.name }};
{% endfor %}
