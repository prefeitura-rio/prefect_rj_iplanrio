{#
{
  "kind": "template",
  "description": "Write what local files hold to a Google Sheet, with the header in the first row.",
  "inputs": {
    "type": "File format: parquet, csv or json.",
    "alias": "SQL literal with the name of the attached database.",
    "secret": "SQL literal with the name of the source secret.",
    "query": "SQL literal with the file path or glob.",
    "spreadsheet": "SQL literal with the spreadsheet id.",
    "sheet": "SQL literal with the sheet name."
  }
}
#}
-- noqa: disable=PRS
{% from "duckdb/macros.sql" import source_table %}
COPY (SELECT * FROM {{ source_table(type, alias, secret, query) }})
TO {{ spreadsheet }} (FORMAT gsheet, SHEET {{ sheet }})
