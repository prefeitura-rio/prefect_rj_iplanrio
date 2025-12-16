

-- Adiciona julgamento aos recursos restantes
with  __dbt__cte__aux_recurso_viagem_recalculada as (


with recursos as (
  select *
  FROM `rj-smtr`.`projeto_subsidio_sppo`.`aux_recurso_viagem_paga`
  where id_julgamento is null
),
viagens_recaculadas as (
    select
        protocolo,
        case
            when perc_conformidade_registros < 50 then 5
            when perc_conformidade_shape < 80 then 6
            when perc_conformidade_distancia <= 50 then 7
            else 8
        end as id_julgamento,
        concat(
            "ID da viagem identificada: ", id_viagem,
            "\nServiços associados ao veículo no período da viagem: ", servico_informado,
            "\nPercentual de conformidade do itinerário: ", perc_conformidade_shape,
            "\nPercentual de conformidade do GPS: ", perc_conformidade_registros,
            "\nPercentual de conformidade da distancia: ", perc_conformidade_distancia
        ) as observacao
    from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade_recurso`
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from `rj-smtr`.`projeto_subsidio_sppo`.`aux_recurso_viagem_paga` r
left join viagens_recaculadas rp
on r.protocolo = rp.protocolo
),recursos as (
  select
    r.* except(id_julgamento),
    ifnull(id_julgamento, 9) as id_julgamento
  from __dbt__cte__aux_recurso_viagem_recalculada r
  
  where data_viagem between date('2022-07-01 00:00:00') and date('2022-07-15 00:00:00')
  
)
-- Preenche campos de julgamento e motivo com base na id_julgamento
select
  r.* except(observacao),
  d.julgamento,
  d.motivo,
  r.observacao
from recursos r
left join (
  select * from rj-smtr.projeto_subsidio_sppo.recurso_julgamento
) d
on r.id_julgamento = d.id_julgamento