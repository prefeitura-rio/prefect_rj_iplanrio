{#
{
  "kind": "template",
  "description": "Create the secret that lets the gsheets extension call the Google Sheets API. The access token is bound as the only parameter.",
  "inputs": {
    "secret": "Secret identifier."
  }
}
#}
-- noqa: disable=PRS
CREATE OR REPLACE SECRET {{ secret }} (TYPE gsheet, PROVIDER access_token, TOKEN ?)
