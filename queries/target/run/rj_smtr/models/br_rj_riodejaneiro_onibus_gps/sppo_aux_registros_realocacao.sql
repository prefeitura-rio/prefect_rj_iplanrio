
        
    

    

    merge into `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_realocacao` as DBT_INTERNAL_DEST
        using (
          

-- 1. Filtra realocações válidas dentro do intervalo de GPS avaliado
with realocacao as (
  select 
    * except(datetime_saida),
    case
      when datetime_saida is null then datetime_operacao
      else datetime_saida
    end as datetime_saida,
  from
    `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_realocacao`
  where
    -- Realocação deve acontecer após o registro de GPS e até 1 hora depois
        datetime_diff(datetime_operacao, datetime_entrada, minute) between 0 and 60
    and 
        data between DATE("2023-11-23T00:00:00")
        and DATE(datetime_add("2023-12-01T23:00:00", interval 1
        hour))
    and 
        datetime_operacao between datetime("2023-11-23T00:00:00")
            and datetime_add("2023-12-01T23:00:00", interval 1 hour)),
-- 2. Altera registros de GPS com servicos realocados
gps as (
  select
    ordem,
    timestamp_gps,
    linha,
    data,
    hora
  from `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_registros`
  where
    data between DATE("2023-11-23T00:00:00") and DATE("2023-12-01T23:00:00")
    and timestamp_gps > "2023-11-23T00:00:00" and timestamp_gps <= "2023-12-01T23:00:00"
),
combinacao as (
  select
    r.id_veiculo,
    g.timestamp_gps,
    g.linha as servico_gps,
    r.servico as servico_realocado,
    r.datetime_operacao as datetime_realocacao,
    g.data,
    g.hora
  from gps g
  inner join realocacao r
  on 
    g.ordem = r.id_veiculo
    and g.linha != r.servico
    and g.timestamp_gps between r.datetime_entrada and r.datetime_saida
)
-- Filtra realocacao mais recente para cada timestamp
select
  * except(rn)
from (
  select 
    *,
    row_number() over (partition by id_veiculo, timestamp_gps order by datetime_realocacao desc) as rn
  from combinacao
)
where rn = 1
        ) as DBT_INTERNAL_SOURCE
        on FALSE

    

    when not matched then insert
        (`id_veiculo`, `timestamp_gps`, `servico_gps`, `servico_realocado`, `datetime_realocacao`, `data`, `hora`)
    values
        (`id_veiculo`, `timestamp_gps`, `servico_gps`, `servico_realocado`, `datetime_realocacao`, `data`, `hora`)


  