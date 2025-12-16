

with __dbt__cte__aux_passageiros_hora as (


select
    data,
    hora,
    modo,
    consorcio,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_transacao,
    tipo_transacao_smtr,
    ifnull(
        case
            when tipo_transacao_smtr = "Gratuidade"
            then tipo_gratuidade
            when tipo_transacao_smtr = "Integração"
            then "Integração"
            when tipo_transacao_smtr = "Transferência"
            then "Transferência"
            else tipo_pagamento
        end,
        "Não Identificado"
    ) as tipo_transacao_detalhe_smtr,
    tipo_gratuidade,
    tipo_pagamento,
    geo_point_transacao
from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao`
where
    id_servico_jae not in ("140", "142")
    and id_operadora != "2"
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
    )
    and tipo_transacao is not null

union all

select
    data,
    hora,
    modo,
    consorcio,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_transacao,
    "RioCard" as tipo_transacao_smtr,
    "RioCard" as tipo_transacao_detalhe_smtr,
    null as tipo_gratuidade,
    "RioCard" as tipo_pagamento,
    st_geogpoint(longitude, latitude) as geo_point_transacao
from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao_riocard`
where
    (id_servico_jae not in ("140", "142") or id_servico_jae is null)
    and (id_operadora != "2" or id_operadora is null)
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
        or modo is null
    )
) /*
consulta as partições a serem atualizadas com base nas transações capturadas entre date_range_start e date_range_end
e as integrações capturadas entre date_range_start e date_range_end
*/



  
    -- Transações Jaé
    

    

    
  


SELECT
  p.* EXCEPT(id_transacao, geo_point_transacao),
  geo.tile_id,
  COUNT(id_transacao) AS quantidade_passageiros,
  '' AS versao
FROM
  __dbt__cte__aux_passageiros_hora p
JOIN
  `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`aux_h3_res9` geo
ON
  ST_CONTAINS(geo.geometry, geo_point_transacao)
WHERE

  
    data = "2000-01-01"
  

GROUP BY
  data,
  hora,
  modo,
  consorcio,
  id_servico_jae,
  servico_jae,
  descricao_servico_jae,
  sentido,
  tipo_transacao_smtr,
  tipo_transacao_detalhe_smtr,
  tipo_gratuidade,
  tipo_pagamento,
  tile_id