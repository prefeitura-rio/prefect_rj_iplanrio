


  


SELECT
    fi.feed_version,
    SAFE_CAST(st.data_versao AS DATE) as feed_start_date,
    fi.feed_end_date,
    SAFE_CAST(st.trip_id AS STRING) trip_id,
    SAFE_CAST(JSON_VALUE(st.content, '$.arrival_time') AS STRING) arrival_time,
    SAFE_CAST(JSON_VALUE(st.content, '$.departure_time') AS DATETIME) departure_time,
    SAFE_CAST(JSON_VALUE(st.content, '$.stop_id') AS STRING) stop_id,
    SAFE_CAST(st.stop_sequence AS INT64) stop_sequence,
    SAFE_CAST(JSON_VALUE(st.content, '$.stop_headsign') AS STRING) stop_headsign,
    SAFE_CAST(JSON_VALUE(st.content, '$.pickup_type') AS STRING) pickup_type,
    SAFE_CAST(JSON_VALUE(st.content, '$.drop_off_type') AS STRING) drop_off_type,
    SAFE_CAST(JSON_VALUE(st.content, '$.continuous_pickup') AS STRING) continuous_pickup,
    SAFE_CAST(JSON_VALUE(st.content, '$.continuous_drop_off') AS STRING) continuous_drop_off,
    SAFE_CAST(JSON_VALUE(st.content, '$.shape_dist_traveled') AS FLOAT64) shape_dist_traveled,
    SAFE_CAST(JSON_VALUE(st.content, '$.timepoint') AS STRING) timepoint,
    '' AS versao_modelo
FROM
    `rj-smtr-staging`.`br_rj_riodejaneiro_gtfs_staging`.`stop_times` st
JOIN
    `rj-smtr`.`gtfs`.`feed_info` fi
ON
    st.data_versao = CAST(fi.feed_start_date AS STRING)
WHERE
        st.data_versao IN ('2024-04-15', '2024-05-03')
        AND fi.feed_start_date IN ('2024-04-15', '2024-05-03')