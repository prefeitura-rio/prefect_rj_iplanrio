
        
    

    

    merge into `rj-smtr-dev`.`data_quality`.`testes_qualidade` as DBT_INTERNAL_DEST
        using (
          

SELECT 
    *,
    DATE('2023-11-07') data_particao,
    COALESCE(11, NULL) hora_particao
FROM `rj-smtr.bq_logs.sppo_veiculo_dia_data_quality` dq

WHERE dq.job_start_time = (SELECT MAX(job_start_time) FROM `rj-smtr.bq_logs.sppo_veiculo_dia_data_quality`)

        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`data_quality_scan`, `data_source`, `data_quality_job_id`, `data_quality_job_configuration`, `job_labels`, `job_start_time`, `job_end_time`, `job_quality_result`, `job_dimension_result`, `job_rows_scanned`, `rule_name`, `rule_description`, `rule_type`, `rule_evaluation_type`, `rule_column`, `rule_dimension`, `rule_threshold_percent`, `rule_parameters`, `rule_passed`, `rule_rows_evaluated`, `rule_rows_passed`, `rule_rows_passed_percent`, `rule_rows_null`, `rule_failed_records_query`, `data_particao`, `hora_particao`)
    values
        (`data_quality_scan`, `data_source`, `data_quality_job_id`, `data_quality_job_configuration`, `job_labels`, `job_start_time`, `job_end_time`, `job_quality_result`, `job_dimension_result`, `job_rows_scanned`, `rule_name`, `rule_description`, `rule_type`, `rule_evaluation_type`, `rule_column`, `rule_dimension`, `rule_threshold_percent`, `rule_parameters`, `rule_passed`, `rule_rows_evaluated`, `rule_rows_passed`, `rule_rows_passed_percent`, `rule_rows_null`, `rule_failed_records_query`, `data_particao`, `hora_particao`)


  