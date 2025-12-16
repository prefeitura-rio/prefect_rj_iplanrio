



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
    and data between DATE("2022-01-01T00:00:00")
    and DATE(datetime_add("2022-01-01T01:00:00", interval 1 hour))
    and datetime_operacao between datetime("2022-01-01T00:00:00")
    and datetime_add("2022-01-01T01:00:00", interval 1 hour)),
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
    data between DATE("2022-01-01T00:00:00") and DATE("2022-01-01T01:00:00")
    and timestamp_gps > "2022-01-01T00:00:00" and timestamp_gps <= "2022-01-01T01:00:00"
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