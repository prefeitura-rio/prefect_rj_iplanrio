

with
    arquivo_retorno as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by dataordem, idconsorcio, idoperadora
                        order by timestamp_captura desc
                    ) as rn
                from `rj-smtr`.`controle_financeiro_staging`.`arquivo_retorno`
                
                    where
                        date(data) between date("2022-01-01T00:00:00") and date(
                            "2022-01-01T01:00:00"
                        )
                
            )
        where rn = 1
    )
select distinct
    dataordem as data_ordem,
    date(datavencimento) as data_pagamento,
    idconsorcio as id_consorcio,
    idoperadora as id_operadora,
    concat(dataordem, idconsorcio, idoperadora) as unique_id,
    valorrealefetivado as valor_pago
from arquivo_retorno
where ispago = true