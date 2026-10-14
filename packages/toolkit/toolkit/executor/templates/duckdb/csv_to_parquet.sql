{#
{
  "kind": "template",
  "description": "Convert one CSV file with a header to Parquet, with every column as text and no Hive columns from the path.",
  "inputs": {
    "source": "SQL literal with the CSV path.",
    "target": "SQL literal with the Parquet path."
  }
}
#}
COPY (SELECT * FROM read_csv({{ source }}, header = true, all_varchar = true, hive_partitioning = false))
TO {{ target }} (FORMAT PARQUET)
