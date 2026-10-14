{#
{
  "kind": "template",
  "description": "Summarize the rows of Parquet files by their content, whatever their order. The row count and two sums of the md5 of each row.",
  "inputs": {
    "source": "SQL literal with the path or glob of the Parquet files."
  }
}
#}
-- noqa: disable=PRS
SELECT
    count(*) AS n,
    coalesce(sum(md5_number_lower(t::VARCHAR)::HUGEINT), 0)::VARCHAR AS low,
    coalesce(sum(md5_number_upper(t::VARCHAR)::HUGEINT), 0)::VARCHAR AS high
FROM read_parquet({{ source }}) AS t
