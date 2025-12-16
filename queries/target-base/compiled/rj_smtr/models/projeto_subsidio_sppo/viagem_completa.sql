
-- 1. Identifica viagens que estão dentro do quadro planejado (por
--    enquanto, consideramos o dia todo).
with viagem_periodo as (
    select distinct
        p.consorcio,
        p.vista,
        p.tipo_dia,
        v.*,
        p.inicio_periodo,
        p.fim_periodo,
        p.id_tipo_trajeto,
        0 as tempo_planejado,
    from (
        select distinct
            consorcio,
            vista,
            data,
            tipo_dia,
            trip_id_planejado as trip_id,
            servico,
            inicio_periodo,
            fim_periodo,
            id_tipo_trajeto
        from
            `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
        
        WHERE
            data = date_sub(date("2022-01-01T01:00:00"), interval 1 day)
        
    ) p
    inner join (
        select distinct * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_conformidade`
        
        WHERE
            data = date_sub(date("2022-01-01T01:00:00"), interval 1 day)
        
    ) v
    on
        v.trip_id = p.trip_id
        and v.data = p.data
),
-- 2. Seleciona viagens completas de acordo com a conformidade
viagem_comp_conf as (
select distinct
    consorcio,
    data,
    tipo_dia,
    id_empresa,
    id_veiculo,
    id_viagem,
    servico_informado,
    servico_realizado,
    vista,
    trip_id,
    shape_id,
    sentido,
    datetime_partida,
    datetime_chegada,
    inicio_periodo,
    fim_periodo,
    case
        when servico_realizado = servico_informado
        then "Completa linha correta"
        else "Completa linha incorreta"
        end as tipo_viagem,
    tempo_viagem,
    tempo_planejado,
    distancia_planejada,
    distancia_aferida,
    n_registros_shape,
    n_registros_total,
    n_registros_minuto,
    perc_conformidade_shape,
    perc_conformidade_distancia,
    perc_conformidade_registros,
    0 as perc_conformidade_tempo,
    -- round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo,
    id_tipo_trajeto,
    '' as versao_modelo,
    CURRENT_DATETIME("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    viagem_periodo v
where (
    perc_conformidade_shape >= 80
)
and (
    perc_conformidade_distancia >= 0
)
and (
    perc_conformidade_registros >= 50
)

),
-- 3. Filtra viagens com mesma chegada e partida pelo maior % de conformidade do shape
filtro_desvio as (
  SELECT
    
    * EXCEPT(rn)
    
FROM (
  SELECT
    *,
    
    ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_partida, datetime_chegada ORDER BY perc_conformidade_shape DESC) AS rn
    
  FROM
    viagem_comp_conf )
WHERE
  rn = 1
),
-- 4. Filtra viagens com partida ou chegada diferentes pela maior distancia percorrida
filtro_partida AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_partida ORDER BY distancia_planejada DESC) AS rn
    FROM
      filtro_desvio )
  WHERE
    rn = 1 )
-- filtro_chegada
SELECT
  * EXCEPT(rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id_veiculo, datetime_chegada ORDER BY distancia_planejada DESC) AS rn
  FROM
    filtro_partida )
WHERE
  rn = 1