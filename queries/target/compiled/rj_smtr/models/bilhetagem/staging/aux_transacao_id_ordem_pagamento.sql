





    
        

        

    


with
    staging as (
        select
            date(data_transacao) as data,
            id as id_transacao,
            id_ordem_ressarcimento as id_ordem_pagamento_servico_operador_dia,
            data_transacao as datetime_transacao,
            data_processamento as datetime_processamento,
            timestamp_captura as datetime_captura
        from `rj-smtr`.`bilhetagem_staging`.`transacao_ordem`
         where 
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
  AND timestamp_captura BETWEEN DATETIME("2022-01-01T00:00:00") AND DATETIME("2022-01-01T01:00:00")
 
    ),
    new_data as (
        select
            s.data,
            s.id_transacao,
            s.datetime_transacao,
            s.datetime_processamento,
            o.data_ordem,
            id_ordem_pagamento_servico_operador_dia,
            o.id_ordem_pagamento_consorcio_operador_dia,
            o.id_ordem_pagamento_consorcio_dia,
            o.id_ordem_pagamento,
            s.datetime_captura
        from staging s
        join
            `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_servico_operador_dia` o using (
                id_ordem_pagamento_servico_operador_dia
            )
    ),
    complete_partitions as (
        select *, 0 as priority
        from new_data
        
    )
select * except (rn, priority)
from
    (
        select
            *,
            row_number() over (
                partition by id_transacao order by datetime_captura desc, priority
            ) as rn
        from complete_partitions
    )
where rn = 1