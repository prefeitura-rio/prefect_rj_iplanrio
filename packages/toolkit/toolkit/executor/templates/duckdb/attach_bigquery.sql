{#
{
  "kind": "template",
  "description": "Attach a BigQuery project read-only, with the credentials of the environment (ADC).",
  "inputs": {
    "alias": "Database alias identifier.",
    "project": "Connection string literal, such as 'project=my-project'."
  }
}
#}
-- noqa: disable=PRS
ATTACH {{ project }} AS {{ alias }} (TYPE bigquery, READ_ONLY)
