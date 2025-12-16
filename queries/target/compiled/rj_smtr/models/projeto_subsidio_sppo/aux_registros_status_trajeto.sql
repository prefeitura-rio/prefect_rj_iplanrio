-- depends_on: `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`

    
    



    




-- 1. Seleciona sinais de GPS registrados no período
with gps as (
    select
        g.* except(longitude, latitude, servico),
        
        servico,
        
        substr(id_veiculo, 2, 3) as id_empresa,
        ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo,
        
    from
        -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g
        `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_sppo` g
    where (
        data between date_sub(date("2022-01-01T01:00:00"), interval 1 day) and date("2022-01-01T01:00:00")
    )
    -- Limita range de busca do gps de D-2 às 00h até D-1 às 3h
    and (
        timestamp_gps between datetime_sub(datetime_trunc("2022-01-01T01:00:00", day), interval 1 day)
        and datetime_add(datetime_trunc("2022-01-01T01:00:00", day), interval 3 hour)
    )
    and status != "Parado garagem"
    
),
-- 2. Classifica a posição do veículo em todos os shapes possíveis de
--    serviços de uma mesma empresa
status_viagem as (
    select
        
        g.data,
        
        g.id_veiculo,
        g.id_empresa,
        g.timestamp_gps,
        timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
        g.posicao_veiculo_geo,
        TRIM(g.servico, " ") as servico_informado,
        s.servico as servico_realizado,
        s.shape_id,
        s.sentido_shape,
        s.shape_id_planejado,
        s.trip_id,
        s.trip_id_planejado,
        s.sentido,
        s.start_pt,
        s.end_pt,
        s.distancia_planejada,
        ifnull(g.distancia,0) as distancia,
        case
            when ST_DWITHIN(g.posicao_veiculo_geo, start_pt, 500)
            then 'start'
            when ST_DWITHIN(g.posicao_veiculo_geo, end_pt, 500)
            then 'end'
            when ST_DWITHIN(g.posicao_veiculo_geo, shape, 500)
            then 'middle'
        else 'out'
        end status_viagem
    from
        gps g
    inner join (
        select
            *
        from
            `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada`
        where
            
            data between date_sub(date("2022-01-01T01:00:00"), interval 1 day) and date("2022-01-01T01:00:00")
            
    ) s
    on
        
        g.data = s.data
        
        and g.servico = s.servico
)
select
    *,
    '' as versao_modelo
from
    status_viagem

