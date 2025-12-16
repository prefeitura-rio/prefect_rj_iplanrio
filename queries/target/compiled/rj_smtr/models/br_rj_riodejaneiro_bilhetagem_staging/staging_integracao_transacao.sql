

select
    data,
    safe_cast(id as string) as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_processamento') as string)
        ),
        'America/Sao_Paulo'
    ) as data_processamento,
    -- Seleciona colunas com os dados de cada transação da integração com os tipos
    -- adequados com base no dicionario de parametros
    
        
            
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.data_transacao_t0'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as data_transacao_t0,
            
        
            
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.data_transacao_ti1'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as data_transacao_t1,
            
        
            
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.data_transacao_ti2'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as data_transacao_t2,
            
        
            
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.data_transacao_ti3'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as data_transacao_t3,
            
        
            
                datetime(
                    parse_timestamp(
                        '%Y-%m-%dT%H:%M:%E*S%Ez',
                        safe_cast(
                            json_value(
                                content,
                                '$.data_transacao_ti4'
                            ) as string
                        )
                    ),
                    'America/Sao_Paulo'
                ) as data_transacao_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_aplicacao_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_aplicacao_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_aplicacao_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_aplicacao_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_aplicacao_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_aplicacao_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_aplicacao_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_aplicacao_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_aplicacao_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_aplicacao_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_consorcio_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_consorcio_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_consorcio_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_consorcio_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_consorcio_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_consorcio_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_consorcio_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_consorcio_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_consorcio_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_consorcio_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_emissor_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_emissor_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_emissor_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_emissor_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_emissor_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_emissor_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_emissor_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_emissor_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_emissor_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_emissor_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_linha_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_linha_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_linha_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_linha_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_linha_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_linha_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_linha_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_linha_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_linha_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_linha_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_matriz_integracao_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_matriz_integracao_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_matriz_integracao_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_matriz_integracao_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_matriz_integracao_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_matriz_integracao_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_matriz_integracao_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_matriz_integracao_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_matriz_integracao_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_matriz_integracao_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_operadora_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_operadora_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_operadora_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_operadora_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_operadora_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_operadora_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_operadora_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_operadora_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_operadora_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_operadora_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_ordem_rateio_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_ordem_rateio_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_ordem_rateio_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_ordem_rateio_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_ordem_rateio_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_ordem_rateio_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_ordem_rateio_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_ordem_rateio_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_ordem_rateio_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_ordem_rateio_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.id_secao_t0'
                    ) as STRING
                ) as id_secao_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_secao_ti1'
                    ) as STRING
                ) as id_secao_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_secao_ti2'
                    ) as STRING
                ) as id_secao_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_secao_ti3'
                    ) as STRING
                ) as id_secao_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_secao_ti4'
                    ) as STRING
                ) as id_secao_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_servico_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_servico_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_servico_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_servico_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_servico_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_servico_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_servico_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_servico_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_servico_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_servico_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_tipo_modal_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_tipo_modal_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_tipo_modal_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_tipo_modal_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_tipo_modal_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_tipo_modal_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_tipo_modal_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_tipo_modal_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.id_tipo_modal_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as id_tipo_modal_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.id_transacao_t0'
                    ) as STRING
                ) as id_transacao_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_transacao_ti1'
                    ) as STRING
                ) as id_transacao_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_transacao_ti2'
                    ) as STRING
                ) as id_transacao_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_transacao_ti3'
                    ) as STRING
                ) as id_transacao_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.id_transacao_ti4'
                    ) as STRING
                ) as id_transacao_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.latitude_trx_t0'
                    ) as FLOAT64
                ) as latitude_trx_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.latitude_trx_ti1'
                    ) as FLOAT64
                ) as latitude_trx_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.latitude_trx_ti2'
                    ) as FLOAT64
                ) as latitude_trx_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.latitude_trx_ti3'
                    ) as FLOAT64
                ) as latitude_trx_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.latitude_trx_ti4'
                    ) as FLOAT64
                ) as latitude_trx_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.longitude_trx_t0'
                    ) as FLOAT64
                ) as longitude_trx_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.longitude_trx_ti1'
                    ) as FLOAT64
                ) as longitude_trx_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.longitude_trx_ti2'
                    ) as FLOAT64
                ) as longitude_trx_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.longitude_trx_ti3'
                    ) as FLOAT64
                ) as longitude_trx_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.longitude_trx_ti4'
                    ) as FLOAT64
                ) as longitude_trx_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.nr_logico_midia_operador_t0'
                    ) as STRING
                ) as nr_logico_midia_operador_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.nr_logico_midia_operador_ti1'
                    ) as STRING
                ) as nr_logico_midia_operador_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.nr_logico_midia_operador_ti2'
                    ) as STRING
                ) as nr_logico_midia_operador_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.nr_logico_midia_operador_ti3'
                    ) as STRING
                ) as nr_logico_midia_operador_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.nr_logico_midia_operador_ti4'
                    ) as STRING
                ) as nr_logico_midia_operador_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.perc_rateio_t0'
                    ) as FLOAT64
                ) as perc_rateio_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.perc_rateio_ti1'
                    ) as FLOAT64
                ) as perc_rateio_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.perc_rateio_ti2'
                    ) as FLOAT64
                ) as perc_rateio_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.perc_rateio_ti3'
                    ) as FLOAT64
                ) as perc_rateio_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.perc_rateio_ti4'
                    ) as FLOAT64
                ) as perc_rateio_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.posicao_validador_t0'
                    ) as STRING
                ) as posicao_validador_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.posicao_validador_ti1'
                    ) as STRING
                ) as posicao_validador_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.posicao_validador_ti2'
                    ) as STRING
                ) as posicao_validador_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.posicao_validador_ti3'
                    ) as STRING
                ) as posicao_validador_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.posicao_validador_ti4'
                    ) as STRING
                ) as posicao_validador_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.sentido_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as sentido_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.sentido_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as sentido_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.sentido_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as sentido_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.sentido_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as sentido_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.sentido_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as sentido_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_compensacao_t0'
                    ) as FLOAT64
                ) as valor_rateio_compensacao_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_compensacao_ti1'
                    ) as FLOAT64
                ) as valor_rateio_compensacao_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_compensacao_ti2'
                    ) as FLOAT64
                ) as valor_rateio_compensacao_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_compensacao_ti3'
                    ) as FLOAT64
                ) as valor_rateio_compensacao_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_compensacao_ti4'
                    ) as FLOAT64
                ) as valor_rateio_compensacao_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_t0'
                    ) as FLOAT64
                ) as valor_rateio_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_ti1'
                    ) as FLOAT64
                ) as valor_rateio_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_ti2'
                    ) as FLOAT64
                ) as valor_rateio_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_ti3'
                    ) as FLOAT64
                ) as valor_rateio_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_rateio_ti4'
                    ) as FLOAT64
                ) as valor_rateio_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_tarifa_t0'
                    ) as FLOAT64
                ) as valor_tarifa_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_tarifa_ti1'
                    ) as FLOAT64
                ) as valor_tarifa_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_tarifa_ti2'
                    ) as FLOAT64
                ) as valor_tarifa_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_tarifa_ti3'
                    ) as FLOAT64
                ) as valor_tarifa_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_tarifa_ti4'
                    ) as FLOAT64
                ) as valor_tarifa_t4,
            
        
    
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_transacao_t0'
                    ) as FLOAT64
                ) as valor_transacao_t0,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_transacao_ti1'
                    ) as FLOAT64
                ) as valor_transacao_t1,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_transacao_ti2'
                    ) as FLOAT64
                ) as valor_transacao_t2,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_transacao_ti3'
                    ) as FLOAT64
                ) as valor_transacao_t3,
            
        
            
                safe_cast(
                    json_value(
                        content, '$.valor_transacao_ti4'
                    ) as FLOAT64
                ) as valor_transacao_t4,
            
        
    
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.veiculo_id_t0'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as veiculo_id_t0,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.veiculo_id_ti1'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as veiculo_id_t1,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.veiculo_id_ti2'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as veiculo_id_t2,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.veiculo_id_ti3'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as veiculo_id_t3,
            
        
            
                replace(
                    safe_cast(
                        json_value(
                            content, '$.veiculo_id_ti4'
                        ) as string
                    ),
                    '.0',
                    ''
                ) as veiculo_id_t4,
            
        
    
    safe_cast(
        json_value(content, '$.id_status_integracao') as string
    ) as id_status_integracao,
    safe_cast(
        json_value(content, '$.valor_transacao_total') as float64
    ) as valor_transacao_total,
    safe_cast(json_value(content, '$.tx_adicional') as string) as tx_adicional
from `rj-smtr-staging`.`source_jae`.`integracao_transacao`