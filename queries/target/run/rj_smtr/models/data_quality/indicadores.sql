
        
        
    

    

    merge into `rj-smtr-dev`.`data_quality`.`indicadores` as DBT_INTERNAL_DEST
        using (
          

WITH 


gps_brt as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validity,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completeness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as accuracy,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "false"
            THEN 0
        ELSE NULL 
        END as volume,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as freshness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as uniqueness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistency,
        
    from rj-smtr.bq_logs.gps_brt_data_quality as dq
    
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.gps_brt_data_quality)
    

),

gps_sppo as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validity,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completeness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as accuracy,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "false"
            THEN 0
        ELSE NULL 
        END as volume,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as freshness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as uniqueness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistency,
        
    from rj-smtr.bq_logs.gps_sppo_data_quality as dq
    
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.gps_sppo_data_quality)
    

),

sppo_veiculo_dia as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validity,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completeness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as accuracy,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "false"
            THEN 0
        ELSE NULL 
        END as volume,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as freshness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as uniqueness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistency,
        
    from rj-smtr.bq_logs.sppo_veiculo_dia_data_quality as dq
    
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.sppo_veiculo_dia_data_quality)
    

),

sumario_servico_dia as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validity,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completeness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as accuracy,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "false"
            THEN 0
        ELSE NULL 
        END as volume,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as freshness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as uniqueness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistency,
        
    from rj-smtr.bq_logs.sumario_servico_dia_data_quality as dq
    
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.sumario_servico_dia_data_quality)
    

),

viagem_completa as (
    select
        data_quality_job_id job_id,
        extract(DATE from job_start_time) data_execucao,
        extract(HOUR from job_start_time) hora_execucao,
        dq.data_source.dataset_id dataset_id,
        dq.data_source.table_id table_id,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VALIDITY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as validity,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.COMPLETENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as completeness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.ACCURACY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as accuracy,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.VOLUME.passed") = "false"
            THEN 0
        ELSE NULL 
        END as volume,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.FRESHNESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as freshness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.UNIQUENESS.passed") = "false"
            THEN 0
        ELSE NULL 
        END as uniqueness,
        
        CASE
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "true"
            THEN 1
            WHEN JSON_VALUE(job_dimension_result,"$.CONSISTENCY.passed") = "false"
            THEN 0
        ELSE NULL 
        END as consistency,
        
    from rj-smtr.bq_logs.viagem_completa_data_quality as dq
    
    WHERE dq.job_start_time = (SELECT MAX(job_start_time) from rj-smtr.bq_logs.viagem_completa_data_quality)
    

)
 



SELECT
    DISTINCT *
from gps_brt
 UNION ALL 

SELECT
    DISTINCT *
from gps_sppo
 UNION ALL 

SELECT
    DISTINCT *
from sppo_veiculo_dia
 UNION ALL 

SELECT
    DISTINCT *
from sumario_servico_dia
 UNION ALL 

SELECT
    DISTINCT *
from viagem_completa


        ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.job_id = DBT_INTERNAL_DEST.job_id
        

    
    when matched then update set
        `job_id` = DBT_INTERNAL_SOURCE.`job_id`,`data_execucao` = DBT_INTERNAL_SOURCE.`data_execucao`,`hora_execucao` = DBT_INTERNAL_SOURCE.`hora_execucao`,`dataset_id` = DBT_INTERNAL_SOURCE.`dataset_id`,`table_id` = DBT_INTERNAL_SOURCE.`table_id`,`validity` = DBT_INTERNAL_SOURCE.`validity`,`completeness` = DBT_INTERNAL_SOURCE.`completeness`,`accuracy` = DBT_INTERNAL_SOURCE.`accuracy`,`volume` = DBT_INTERNAL_SOURCE.`volume`,`freshness` = DBT_INTERNAL_SOURCE.`freshness`,`uniqueness` = DBT_INTERNAL_SOURCE.`uniqueness`,`consistency` = DBT_INTERNAL_SOURCE.`consistency`
    

    when not matched then insert
        (`job_id`, `data_execucao`, `hora_execucao`, `dataset_id`, `table_id`, `validity`, `completeness`, `accuracy`, `volume`, `freshness`, `uniqueness`, `consistency`)
    values
        (`job_id`, `data_execucao`, `hora_execucao`, `dataset_id`, `table_id`, `validity`, `completeness`, `accuracy`, `volume`, `freshness`, `uniqueness`, `consistency`)


  