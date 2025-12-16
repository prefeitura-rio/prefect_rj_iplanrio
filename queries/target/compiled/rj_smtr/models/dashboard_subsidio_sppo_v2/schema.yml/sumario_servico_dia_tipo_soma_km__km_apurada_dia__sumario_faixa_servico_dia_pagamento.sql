
    -- depends_on: `rj-smtr`.`subsidio`.`valor_km_tipo_viagem`
    with
        kms as (
            select
                * except (km_apurada_faixa),
                km_apurada_faixa,
                round(
                            coalesce(km_apurada_autuado_alterar_itinerario, 0) +
                            coalesce(km_apurada_autuado_ar_inoperante, 0) +
                            coalesce(km_apurada_autuado_iluminacao_insuficiente, 0) +
                            coalesce(km_apurada_autuado_limpezaequipamento, 0) +
                            coalesce(km_apurada_autuado_n_atender_solicitacao_parada, 0) +
                            coalesce(km_apurada_autuado_n_concluir_itinerario, 0) +
                            coalesce(km_apurada_autuado_seguranca, 0) +
                            coalesce(km_apurada_autuado_vista_inoperante, 0) +
                            coalesce(km_apurada_detectado_com_ar_inoperante, 0) +
                            coalesce(km_apurada_lacrado, 0) +
                            coalesce(km_apurada_n_autorizado_ausencia_ar_condicionado, 0) +
                            coalesce(km_apurada_n_autorizado_capacidade, 0) +
                            coalesce(km_apurada_n_licenciado, 0) +
                            coalesce(km_apurada_n_vistoriado, 0) +
                            coalesce(km_apurada_registrado_com_ar_inoperante, 0) +
                            coalesce(km_apurada_sem_transacao, 0) +
                            coalesce(km_apurada_validador_associado_incorretamente, 0) +
                            coalesce(km_apurada_validador_fechado, 0)
                        + coalesce(km_apurada_total_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_total_licenciado_com_ar_n_autuado, 0),
                    2
                ) as km_apurada2
            from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo_v2`.`sumario_faixa_servico_dia_pagamento` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00'))
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        )
    select *, abs(km_apurada2 -km_apurada_faixa) as dif
    from kms
    where abs(km_apurada2 -km_apurada_faixa) > 0.02