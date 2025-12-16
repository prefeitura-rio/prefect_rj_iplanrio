



WITH 
last_jobs as (
    SELECT DISTINCT
        data_quality_job_id job_id,
        row_number() over (partition by data_particao order by job_start_time DESC) rn
        from `rj-smtr-dev`.`data_quality`.`testes_qualidade`
),
gps_sppo as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        data_particao,
        hora_particao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validade,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completude,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as acuracia,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as atualizacao,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as exclusividade,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistencia,
        
    from `rj-smtr-dev`.`data_quality`.`testes_qualidade` as dq
)
SELECT
    DISTINCT t.*
from gps_sppo t
join (SELECT * FROM last_jobs WHERE rn=1) l
ON l.job_id = t.job_id