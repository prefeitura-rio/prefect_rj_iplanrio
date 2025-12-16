-- depends_on: `rj-smtr`.`veiculo_staging`.`infracao_data_versao_efetiva`



    
    
    

with
    infracao as (
        select * except (data), date(data) as data
        from `rj-smtr`.`monitoramento_staging`.`infracao` as i
        
            where
                date(data) between date("None") and date(
                    "None"
                )
        
    )
select
    *,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "" as versao
from infracao
where data <= '2025-03-31'
qualify
    row_number() over (
        partition by data, id_auto_infracao order by timestamp_captura desc
    )
    = 1