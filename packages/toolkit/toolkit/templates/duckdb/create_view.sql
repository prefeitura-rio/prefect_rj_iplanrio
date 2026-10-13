{#
{
  "kind": "template",
  "description": "Create a temporary view over a SELECT, so the next cleaning step can read it.",
  "inputs": {
    "view": "View identifier.",
    "select": "The SELECT statement the view runs."
  }
}
#}
-- noqa: disable=PRS
CREATE OR REPLACE TEMP VIEW {{ view }} AS
{{ select }}
