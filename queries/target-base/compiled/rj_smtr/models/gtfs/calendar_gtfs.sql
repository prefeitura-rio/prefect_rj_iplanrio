


  


SELECT
  fi.feed_version,
  SAFE_CAST(c.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(c.service_id AS STRING) service_id,
  SAFE_CAST(JSON_VALUE(c.content, '$.monday') AS STRING) monday,
  SAFE_CAST(JSON_VALUE(c.content, '$.tuesday') AS STRING) tuesday,
  SAFE_CAST(JSON_VALUE(c.content, '$.wednesday') AS STRING) wednesday,
  SAFE_CAST(JSON_VALUE(c.content, '$.thursday') AS STRING) thursday,
  SAFE_CAST(JSON_VALUE(c.content, '$.friday') AS STRING) friday,
  SAFE_CAST(JSON_VALUE(c.content, '$.saturday') AS STRING) saturday,
  SAFE_CAST(JSON_VALUE(c.content, '$.sunday') AS STRING) sunday,
  PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(c.content, '$.start_date') AS STRING)) start_date,
  PARSE_DATE('%Y%m%d', SAFE_CAST(JSON_VALUE(c.content, '$.end_date') AS STRING)) end_date,
  '' AS versao_modelo
 FROM `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`calendar` c
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  c.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    c.data_versao IN ('2024-04-15', '2024-05-03')
    AND fi.feed_start_date IN ('2024-04-15', '2024-05-03')