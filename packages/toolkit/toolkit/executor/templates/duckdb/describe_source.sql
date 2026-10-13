{#
{
  "kind": "template",
  "description": "Describe the columns of a source expression.",
  "inputs": {
    "source": "DuckDB FROM expression."
  }
}
#}
-- noqa: disable=PRS
DESCRIBE SELECT * FROM {{ source }}
