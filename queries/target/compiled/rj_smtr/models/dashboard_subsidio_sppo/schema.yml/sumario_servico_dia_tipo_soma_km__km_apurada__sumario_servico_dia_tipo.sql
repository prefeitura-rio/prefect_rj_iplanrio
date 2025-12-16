
    -- depends_on: `rj-smtr`.`subsidio`.`valor_km_tipo_viagem`
    with
        kms as (
            select
                * except (km_apurada),
                km_apurada,
                round(coalesce(km_apurada_registrado_com_ar_inoperante, 0)
                        + coalesce(km_apurada_n_licenciado, 0)
                        + coalesce(km_apurada_autuado_ar_inoperante, 0)
                        + coalesce(km_apurada_autuado_seguranca, 0)
                        + coalesce(km_apurada_autuado_limpezaequipamento, 0)
                        + coalesce(km_apurada_licenciado_sem_ar_n_autuado, 0)
                        + coalesce(km_apurada_licenciado_com_ar_n_autuado, 0)
                        + coalesce(km_apurada_n_vistoriado, 0)
                        + coalesce(km_apurada_sem_transacao, 0),
                    2
                ) as km_apurada2
            from 
    
        
        

        

        
            
            
            
            
        
            
            
            
            
        
            
            
            
            
        
        (select * from `rj-smtr`.`dashboard_subsidio_sppo`.`sumario_servico_dia_tipo` where data between date('2022-01-01T00:00:00') and date('2022-01-01T01:00:00') and data < date('2024-08-16'))
            where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-01T01:00:00"
                )
        )
    select *, abs(km_apurada2 -km_apurada) as dif
    from kms
    where abs(km_apurada2 -km_apurada) > 0.02