


  


SELECT
  fi.feed_version,
  SAFE_CAST(s.data_versao AS DATE) as feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(s.shape_id AS STRING) shape_id,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_pt_lat') AS FLOAT64) shape_pt_lat,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_pt_lon') AS FLOAT64) shape_pt_lon,
  SAFE_CAST(s.shape_pt_sequence AS INT64) shape_pt_sequence,
  SAFE_CAST(JSON_VALUE(s.content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
  '' AS versao_modelo
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`shapes` s
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  s.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    s.data_versao IN ('2024-12-23', '2024-12-31')
    AND fi.feed_start_date IN ('2024-12-23', '2024-12-31')