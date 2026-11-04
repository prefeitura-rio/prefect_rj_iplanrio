{#
{
  "kind": "template",
  "description": "Set the query timeout of the SQL Server extension, which cuts every query at 30 seconds by default.",
  "inputs": {
    "seconds": "Literal with the timeout in seconds, or 0 for none."
  }
}
#}
-- noqa: disable=PRS
SET mssql_query_timeout = {{ seconds }};
