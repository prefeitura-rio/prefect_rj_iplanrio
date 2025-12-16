






    


with
    transacao as (
        select
            data_ordem,
            data as data_transacao,
            id_transacao,
            modo,
            consorcio,
            id_operadora,
            id_servico_jae,
            valor_transacao as valor_transacao_rateio,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento_consorcio_operador_dia
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao`
        
    ),
    integracao as (
        select
            data_ordem,
            data as data_transacao,
            id_transacao,
            modo,
            consorcio,
            id_operadora,
            id_servico_jae,
            ifnull(sum(valor_rateio_compensacao), 0) as valor_transacao_rateio,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio as id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento_consorcio_operadora
            as id_ordem_pagamento_consorcio_operador_dia
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
        
        group by all
    ),
    transacao_integracao as (
        select *
        from transacao
        union all
        select *
        from integracao
    ),
    ordem_agrupada as (
        select
            data_ordem,
            data_transacao,
            id_transacao,
            modo,
            consorcio,
            id_operadora,
            id_servico_jae,
            sum(valor_transacao_rateio) as valor_transacao_rateio,
            id_ordem_pagamento,
            id_ordem_pagamento_consorcio_dia,
            id_ordem_pagamento_consorcio_operador_dia
        from transacao_integracao
        where data_ordem is not null
        group by all
    ),
    particao_completa as (
        select *, 0 as priority
        from ordem_agrupada
        
    )
select
    * except (priority),
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from particao_completa
qualify row_number() over (partition by id_transacao, data_ordem order by priority) = 1