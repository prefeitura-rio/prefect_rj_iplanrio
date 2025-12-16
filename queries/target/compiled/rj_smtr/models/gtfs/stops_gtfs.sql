


  


SELECT
    fi.feed_version,
    SAFE_CAST(s.data_versao AS DATE) as feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(s.stop_id AS STRING) stop_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_code') AS STRING) stop_code,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_name') AS STRING) stop_name,
    SAFE_CAST(JSON_VALUE(s.content, '$.tts_stop_name') AS STRING) tts_stop_name,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_desc') AS STRING) stop_desc,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_lat') AS FLOAT64) stop_lat,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_lon') AS FLOAT64) stop_lon,
    SAFE_CAST(JSON_VALUE(s.content, '$.zone_id') AS STRING) zone_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_url') AS STRING) stop_url,
    SAFE_CAST(SAFE_CAST(SAFE_CAST(JSON_VALUE(s.content, '$.location_type') AS FLOAT64) AS INT64) AS STRING) location_type,
    SAFE_CAST(JSON_VALUE(s.content, '$.parent_station') AS STRING) parent_station,
    SAFE_CAST(JSON_VALUE(s.content, '$.stop_timezone') AS STRING) stop_timezone,
    SAFE_CAST(JSON_VALUE(s.content, '$.wheelchair_boarding') AS STRING) wheelchair_boarding,
    SAFE_CAST(JSON_VALUE(s.content, '$.level_id') AS STRING) level_id,
    SAFE_CAST(JSON_VALUE(s.content, '$.platform_code') AS STRING) platform_code,
    '' AS versao_modelo
FROM
    `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`stops` s
JOIN
    `rj-smtr`.`gtfs`.`feed_info` fi
ON
    s.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
        s.data_versao IN ('2024-12-23', '2024-12-31')
        AND fi.feed_start_date IN ('2024-12-23', '2024-12-31')