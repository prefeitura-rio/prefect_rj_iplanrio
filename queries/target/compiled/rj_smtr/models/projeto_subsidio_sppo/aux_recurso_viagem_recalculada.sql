

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