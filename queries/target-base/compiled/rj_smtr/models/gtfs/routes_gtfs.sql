


  


SELECT
  fi.feed_version,
  SAFE_CAST(r.data_versao AS DATE) feed_start_date,
  fi.feed_end_date,
  SAFE_CAST(r.route_id AS STRING) route_id,
  SAFE_CAST(JSON_VALUE(r.content, '$.agency_id') AS STRING) agency_id,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_short_name') AS STRING) route_short_name,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_long_name') AS STRING) route_long_name,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_desc') AS STRING) route_desc,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_type') AS STRING) route_type,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_url') AS STRING) route_url,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_color') AS STRING) route_color,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_text_color') AS STRING) route_text_color,
  SAFE_CAST(JSON_VALUE(r.content, '$.route_sort_order') AS INT64) route_sort_order,
  SAFE_CAST(JSON_VALUE(r.content, '$.continuous_pickup') AS STRING) continuous_pickup,
  SAFE_CAST(JSON_VALUE(r.content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
  SAFE_CAST(JSON_VALUE(r.content, '$.network_id') AS STRING) network_id,
  '' AS versao_modelo
 FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`routes` r
JOIN
  `rj-smtr`.`gtfs`.`feed_info` fi
ON
  r.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
    r.data_versao IN ('2024-04-15', '2024-05-03')
    AND fi.feed_start_date IN ('2024-04-15', '2024-05-03')