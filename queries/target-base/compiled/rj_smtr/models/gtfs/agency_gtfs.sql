


  


SELECT
  fi.feed_version,
  SAFE_CAST(a.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(a.agency_id AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_name') AS STRING) agency_name,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_url') AS STRING) agency_url,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_timezone') AS STRING) agency_timezone,
  SAFE_CAST(JSON_VALUE(a.content, '$.agency_lang') AS STRING) agency_lang,
  '' AS versao_modelo
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`agency` a
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  a.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    a.data_versao IN ('2024-04-15', '2024-05-03')
    AND fi.feed_start_date IN ('2024-04-15', '2024-05-03')