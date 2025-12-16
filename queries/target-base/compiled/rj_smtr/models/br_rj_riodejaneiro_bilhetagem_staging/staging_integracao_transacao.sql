

SELECT
  data,
  SAFE_CAST(id AS STRING) AS id,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo") AS timestamp_captura,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_inclusao') AS STRING)), 'America/Sao_Paulo') AS data_inclusao,
  DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', SAFE_CAST(JSON_VALUE(content, '$.data_processamento') AS STRING)), 'America/Sao_Paulo') AS data_processamento,
  -- Seleciona colunas com os dados de cada transação da integração com os tipos adequados com base no dicionario de parametros
  
    
      
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.data_transacao_t0') AS STRING)
          ),
          'America/Sao_Paulo') AS data_transacao_t0,
      
    
      
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.data_transacao_ti1') AS STRING)
          ),
          'America/Sao_Paulo') AS data_transacao_t1,
      
    
      
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.data_transacao_ti2') AS STRING)
          ),
          'America/Sao_Paulo') AS data_transacao_t2,
      
    
      
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.data_transacao_ti3') AS STRING)
          ),
          'America/Sao_Paulo') AS data_transacao_t3,
      
    
      
        DATETIME(
          PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            SAFE_CAST(JSON_VALUE(content, '$.data_transacao_ti4') AS STRING)
          ),
          'America/Sao_Paulo') AS data_transacao_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_aplicacao_t0') AS STRING), '.0', '') AS id_aplicacao_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_aplicacao_ti1') AS STRING), '.0', '') AS id_aplicacao_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_aplicacao_ti2') AS STRING), '.0', '') AS id_aplicacao_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_aplicacao_ti3') AS STRING), '.0', '') AS id_aplicacao_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_aplicacao_ti4') AS STRING), '.0', '') AS id_aplicacao_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_consorcio_t0') AS STRING), '.0', '') AS id_consorcio_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_consorcio_ti1') AS STRING), '.0', '') AS id_consorcio_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_consorcio_ti2') AS STRING), '.0', '') AS id_consorcio_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_consorcio_ti3') AS STRING), '.0', '') AS id_consorcio_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_consorcio_ti4') AS STRING), '.0', '') AS id_consorcio_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_emissor_t0') AS STRING), '.0', '') AS id_emissor_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_emissor_ti1') AS STRING), '.0', '') AS id_emissor_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_emissor_ti2') AS STRING), '.0', '') AS id_emissor_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_emissor_ti3') AS STRING), '.0', '') AS id_emissor_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_emissor_ti4') AS STRING), '.0', '') AS id_emissor_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_linha_t0') AS STRING), '.0', '') AS id_linha_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_linha_ti1') AS STRING), '.0', '') AS id_linha_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_linha_ti2') AS STRING), '.0', '') AS id_linha_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_linha_ti3') AS STRING), '.0', '') AS id_linha_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_linha_ti4') AS STRING), '.0', '') AS id_linha_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_matriz_integracao_t0') AS STRING), '.0', '') AS id_matriz_integracao_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_matriz_integracao_ti1') AS STRING), '.0', '') AS id_matriz_integracao_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_matriz_integracao_ti2') AS STRING), '.0', '') AS id_matriz_integracao_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_matriz_integracao_ti3') AS STRING), '.0', '') AS id_matriz_integracao_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_matriz_integracao_ti4') AS STRING), '.0', '') AS id_matriz_integracao_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_operadora_t0') AS STRING), '.0', '') AS id_operadora_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_operadora_ti1') AS STRING), '.0', '') AS id_operadora_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_operadora_ti2') AS STRING), '.0', '') AS id_operadora_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_operadora_ti3') AS STRING), '.0', '') AS id_operadora_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_operadora_ti4') AS STRING), '.0', '') AS id_operadora_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_ordem_rateio_t0') AS STRING), '.0', '') AS id_ordem_rateio_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_ordem_rateio_ti1') AS STRING), '.0', '') AS id_ordem_rateio_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_ordem_rateio_ti2') AS STRING), '.0', '') AS id_ordem_rateio_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_ordem_rateio_ti3') AS STRING), '.0', '') AS id_ordem_rateio_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_ordem_rateio_ti4') AS STRING), '.0', '') AS id_ordem_rateio_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_secao_t0') AS STRING) AS id_secao_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_secao_ti1') AS STRING) AS id_secao_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_secao_ti2') AS STRING) AS id_secao_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_secao_ti3') AS STRING) AS id_secao_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_secao_ti4') AS STRING) AS id_secao_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_servico_t0') AS STRING), '.0', '') AS id_servico_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_servico_ti1') AS STRING), '.0', '') AS id_servico_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_servico_ti2') AS STRING), '.0', '') AS id_servico_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_servico_ti3') AS STRING), '.0', '') AS id_servico_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_servico_ti4') AS STRING), '.0', '') AS id_servico_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_t0') AS STRING), '.0', '') AS id_tipo_modal_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_ti1') AS STRING), '.0', '') AS id_tipo_modal_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_ti2') AS STRING), '.0', '') AS id_tipo_modal_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_ti3') AS STRING), '.0', '') AS id_tipo_modal_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.id_tipo_modal_ti4') AS STRING), '.0', '') AS id_tipo_modal_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_transacao_t0') AS STRING) AS id_transacao_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_transacao_ti1') AS STRING) AS id_transacao_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_transacao_ti2') AS STRING) AS id_transacao_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_transacao_ti3') AS STRING) AS id_transacao_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.id_transacao_ti4') AS STRING) AS id_transacao_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx_t0') AS FLOAT64) AS latitude_trx_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx_ti1') AS FLOAT64) AS latitude_trx_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx_ti2') AS FLOAT64) AS latitude_trx_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx_ti3') AS FLOAT64) AS latitude_trx_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.latitude_trx_ti4') AS FLOAT64) AS latitude_trx_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx_t0') AS FLOAT64) AS longitude_trx_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx_ti1') AS FLOAT64) AS longitude_trx_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx_ti2') AS FLOAT64) AS longitude_trx_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx_ti3') AS FLOAT64) AS longitude_trx_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.longitude_trx_ti4') AS FLOAT64) AS longitude_trx_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador_t0') AS STRING) AS nr_logico_midia_operador_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador_ti1') AS STRING) AS nr_logico_midia_operador_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador_ti2') AS STRING) AS nr_logico_midia_operador_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador_ti3') AS STRING) AS nr_logico_midia_operador_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.nr_logico_midia_operador_ti4') AS STRING) AS nr_logico_midia_operador_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_t0') AS FLOAT64) AS perc_rateio_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_ti1') AS FLOAT64) AS perc_rateio_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_ti2') AS FLOAT64) AS perc_rateio_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_ti3') AS FLOAT64) AS perc_rateio_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.perc_rateio_ti4') AS FLOAT64) AS perc_rateio_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador_t0') AS STRING) AS posicao_validador_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador_ti1') AS STRING) AS posicao_validador_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador_ti2') AS STRING) AS posicao_validador_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador_ti3') AS STRING) AS posicao_validador_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.posicao_validador_ti4') AS STRING) AS posicao_validador_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.sentido_t0') AS STRING), '.0', '') AS sentido_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.sentido_ti1') AS STRING), '.0', '') AS sentido_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.sentido_ti2') AS STRING), '.0', '') AS sentido_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.sentido_ti3') AS STRING), '.0', '') AS sentido_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.sentido_ti4') AS STRING), '.0', '') AS sentido_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_t0') AS FLOAT64) AS valor_rateio_compensacao_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_ti1') AS FLOAT64) AS valor_rateio_compensacao_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_ti2') AS FLOAT64) AS valor_rateio_compensacao_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_ti3') AS FLOAT64) AS valor_rateio_compensacao_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_compensacao_ti4') AS FLOAT64) AS valor_rateio_compensacao_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_t0') AS FLOAT64) AS valor_rateio_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_ti1') AS FLOAT64) AS valor_rateio_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_ti2') AS FLOAT64) AS valor_rateio_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_ti3') AS FLOAT64) AS valor_rateio_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_rateio_ti4') AS FLOAT64) AS valor_rateio_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa_t0') AS FLOAT64) AS valor_tarifa_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa_ti1') AS FLOAT64) AS valor_tarifa_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa_ti2') AS FLOAT64) AS valor_tarifa_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa_ti3') AS FLOAT64) AS valor_tarifa_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_tarifa_ti4') AS FLOAT64) AS valor_tarifa_t4,
      
    
  
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_t0') AS FLOAT64) AS valor_transacao_t0,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_ti1') AS FLOAT64) AS valor_transacao_t1,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_ti2') AS FLOAT64) AS valor_transacao_t2,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_ti3') AS FLOAT64) AS valor_transacao_t3,
      
    
      
        SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_ti4') AS FLOAT64) AS valor_transacao_t4,
      
    
  
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.veiculo_id_t0') AS STRING), '.0', '') AS veiculo_id_t0,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.veiculo_id_ti1') AS STRING), '.0', '') AS veiculo_id_t1,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.veiculo_id_ti2') AS STRING), '.0', '') AS veiculo_id_t2,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.veiculo_id_ti3') AS STRING), '.0', '') AS veiculo_id_t3,
      
    
      
        REPLACE(SAFE_CAST(JSON_VALUE(content, '$.veiculo_id_ti4') AS STRING), '.0', '') AS veiculo_id_t4,
      
    
  
  SAFE_CAST(JSON_VALUE(content, '$.id_status_integracao') AS STRING) AS id_status_integracao,
  SAFE_CAST(JSON_VALUE(content, '$.valor_transacao_total') AS FLOAT64) AS valor_transacao_total,
  SAFE_CAST(JSON_VALUE(content, '$.tx_adicional') AS STRING) AS tx_adicional
FROM
  `rj-smtr-staging`.`br_rj_riodejaneiro_bilhetagem_staging`.`integracao_transacao`