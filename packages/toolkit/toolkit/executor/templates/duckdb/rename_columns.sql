{#
{
  "kind": "template",
  "description": "Rename columns of a table of the connection.",
  "inputs": {
    "table": "Identifier of the table.",
    "renames": "Each column as a mapping with its source and target identifiers."
  }
}
#}
-- noqa: disable=PRS
{% for rename in renames %}
ALTER TABLE {{ table }} RENAME COLUMN {{ rename.source }} TO {{ rename.target }};
{% endfor %}
