

with  __dbt__cte__aux_recurso_incorreto as (


-- 1. Corrije preenchimento de dados
with recursos_tratada as (
  select
    timestamp_captura,
    data_recurso,
    protocolo,
    data_viagem,
    datetime(data_viagem, hora_partida) as datetime_partida,
    case
      when hora_partida > hora_chegada and hora_chegada <= time(03,00,00)
      then datetime(date_add(data_viagem, interval 1 day), hora_chegada)
      else datetime(data_viagem, hora_chegada)
    end as datetime_chegada,
    case 
      -- caso 1: linha regular => SA
      when tipo_servico_extra is null and tipo_servico = "SA" then linha
      -- caso 2: lecd
      when tipo_servico_extra = "LECD" and tipo_servico = "SA" then concat(tipo_servico_extra, linha)
      -- caso 3: linha com servico nao regular => SV/SVB
      when tipo_servico_extra is null and tipo_servico != "SA" then concat(tipo_servico, linha)
      when tipo_servico_extra is not null and STARTS_WITH(tipo_servico_extra, tipo_servico) then concat(tipo_servico_extra, linha)
      else concat("Não identificado: ", servico, " / ", tipo_servico)
    end as servico,
    sentido,
    id_veiculo
  from `rj-smtr`.`projeto_subsidio_sppo`.`recurso_filtrada`
),
-- 2. Avalia informações incorretas
recursos_incorretos as (
  select
    *,
    case 
      when datetime_partida > datetime_chegada then "Fim da viagem incorreto."
      when datetime_partida > data_recurso then "Início da viagem incorreto."
      when servico like "Não identificado:%" then "Linha e tipo de serviço não correspondem."
      when sentido not in ("I", "V", "C") then "Sentido incorreto."
      when length(id_veiculo) != 5 and length(REGEXP_EXTRACT(id_veiculo, r'[0-9]+')) != 5 then "Número de ordem incorreto - não possui 5 dígitos."
    end as observacao
  from recursos_tratada
)
select
  *,
  case 
    when observacao is not null
    then 1
    else null 
  end as id_julgamento
from recursos_incorretos
),  __dbt__cte__aux_recurso_viagem_nao_planejada as (


with recursos as (
  select *
  FROM __dbt__cte__aux_recurso_incorreto
  where id_julgamento is null
  
),
servico_planejado as (
    select data, servico, sentido
    FROM `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
    ),
recursos_nao_planejados as (
    select
        r.protocolo,
        1 as id_julgamento,
        "Serviço e sentido não planejados no subsídio na respectiva data da viagem." as observacao
    FROM recursos r
    left join servico_planejado s
    on date(r.datetime_partida) = s.data
    and r.servico = s.servico
    and r.sentido = s.sentido
    where s.servico is null
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from __dbt__cte__aux_recurso_incorreto r 
left join recursos_nao_planejados rp
on r.protocolo = rp.protocolo
),  __dbt__cte__aux_recurso_fora_prazo as (


with recursos as (
  select *
  FROM __dbt__cte__aux_recurso_viagem_nao_planejada
  where id_julgamento is null
),
recurso_prazo as (
  select 
    r.protocolo,
    2 as id_julgamento,
    concat("Prazo (30 dias após apuração):", data_fim_recurso) as observacao
  from 
    recursos r
  inner join 
    rj-smtr.projeto_subsidio_sppo.recurso_prazo p
  on 
    date(r.datetime_partida) between p.data_inicio_viagem and p.data_fim_viagem
  where extract(date from data_recurso) > date(data_fim_recurso)
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from __dbt__cte__aux_recurso_viagem_nao_planejada r 
left join recurso_prazo rp
on r.protocolo = rp.protocolo
),  __dbt__cte__aux_recurso_duplicado as (


with recursos as (
    select *
    from __dbt__cte__aux_recurso_fora_prazo
    where id_julgamento is null
),
recursos_duplicados as (
    SELECT
        s1.protocolo,
        3 as id_julgamento,
        concat("Pedidos sobrepostos: ", string_agg(s2.protocolo, ", ")) as observacao
    FROM recursos s1
    inner join recursos s2
    on 
        s1.id_veiculo = s2.id_veiculo
        and s1.datetime_partida <= s2.datetime_partida
        and s2.datetime_partida < s1.datetime_chegada
        and s1.protocolo != s2.protocolo
    group by 1,2
)
select
    r.* except(id_julgamento, observacao),
    coalesce(r.id_julgamento, rd.id_julgamento) as id_julgamento,
    coalesce(r.observacao, rd.observacao) as observacao
from __dbt__cte__aux_recurso_fora_prazo r
left join recursos_duplicados rd
on r.protocolo = rd.protocolo
),recursos as (
  select *
  FROM __dbt__cte__aux_recurso_duplicado
  where id_julgamento is null
),
-- 1. Avalia recursos cuja viagem ja foi paga
viagens as (
    select * 
    from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
    
      where data between date('2022-07-01 00:00:00') and date('2022-07-15 00:00:00')
    
),
recursos_pagos as (
  select
    r.protocolo,
    4 as id_julgamento,
    concat("Viagem(s) paga(s): ", string_agg(v.id_viagem, ", ")) as observacao
  FROM recursos r
  inner join viagens v
  on r.id_veiculo = substr(v.id_veiculo,2,6)
  and v.datetime_partida <= r.datetime_chegada
  and v.datetime_chegada >= r.datetime_partida
  group by 1,2
)
select
  r.* except(id_julgamento, observacao),
  coalesce(r.id_julgamento, rp.id_julgamento) as id_julgamento,
  coalesce(r.observacao, rp.observacao) as observacao
from __dbt__cte__aux_recurso_duplicado r
left join recursos_pagos rp
on r.protocolo = rp.protocolo