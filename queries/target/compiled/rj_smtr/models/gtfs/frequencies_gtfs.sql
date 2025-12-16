


  


SELECT
  fi.feed_version,
  SAFE_CAST(f.data_versao AS DATE) as feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(f.trip_id AS STRING) trip_id,
  SAFE_CAST(f.start_time AS STRING) start_time,
  SAFE_CAST(JSON_VALUE(f.content, '$.end_time') AS STRING) end_time,
  SAFE_CAST(JSON_VALUE(f.content, '$.headway_secs') AS INT64) headway_secs,
  SAFE_CAST(JSON_VALUE(f.content, '$.exact_times') AS STRING) exact_times,
  '' AS versao_modelo
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`frequencies` f
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  f.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    f.data_versao IN ('2024-12-23', '2024-12-31')
    AND fi.feed_start_date IN ('2024-12-23', '2024-12-31')