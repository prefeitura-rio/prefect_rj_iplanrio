

SELECT 
    *,
    DATE('2023-11-07') data_particao,
    COALESCE(11, NULL) hora_particao
FROM `rj-smtr.bq_logs.sppo_veiculo_dia_data_quality` dq

WHERE dq.job_start_time = (SELECT MAX(job_start_time) FROM `rj-smtr.bq_logs.sppo_veiculo_dia_data_quality`)
