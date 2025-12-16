


  


SELECT
  fi.feed_version,
  SAFE_CAST(fr.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(fr.fare_id AS STRING) fare_id,
  SAFE_CAST(fr.route_id AS STRING) route_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.origin_id') AS STRING) origin_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.destination_id') AS STRING) destination_id,
  SAFE_CAST(JSON_VALUE(fr.content, '$.contains_id') AS STRING) contains_id,
  '' AS versao_modelo
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`fare_rules` fr
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  fr.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    fr.data_versao IN ('2024-04-15', '2024-05-03')
    AND fi.feed_start_date IN ('2024-04-15', '2024-05-03')