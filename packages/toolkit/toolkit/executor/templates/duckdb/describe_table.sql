{#
{
  "kind": "template",
  "description": "Describe the columns of a table of the connection.",
  "inputs": {
    "table": "Identifier of the table."
  }
}
#}
-- noqa: disable=PRS
DESCRIBE {{ table }}
