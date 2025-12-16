


  



SELECT
  fi.feed_version,
  SAFE_CAST(cd.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(cd.service_id AS STRING) service_id,
  PARSE_DATE('%Y%m%d', SAFE_CAST(cd.DATE AS STRING)) DATE,
  SAFE_CAST(JSON_VALUE(cd.content, '$.exception_type') AS STRING) exception_type,
  '' AS versao_modelo
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`calendar_dates` cd
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  cd.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    cd.data_versao IN ('2024-12-23', '2024-12-31')
    AND fi.feed_start_date IN ('2024-12-23', '2024-12-31')