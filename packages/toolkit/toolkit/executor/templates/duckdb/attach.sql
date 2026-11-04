{#
{
  "kind": "template",
  "description": "Attach a source database as an alias, with the credentials of its secret.",
  "inputs": {
    "secret": "Secret identifier.",
    "alias": "Database alias identifier.",
    "type": "Scanner type: postgres or mssql.",
    "read_only": "Whether to attach read-only."
  }
}
#}
-- noqa: disable=PRS
ATTACH '' AS {{ alias }} (TYPE {{ type }}, SECRET {{ secret }}{% if read_only %}, READ_ONLY{% endif %})
