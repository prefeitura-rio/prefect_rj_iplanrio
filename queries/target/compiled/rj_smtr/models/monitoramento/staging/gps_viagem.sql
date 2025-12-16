




with
    viagem as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            modo,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            fonte_gps
        from `rj-smtr`.`monitoramento`.`viagem_informada`
        
        
            where
                data between date('2022-01-01T00:00:00') and date(
                    '2022-01-01T01:00:00'
                )
        
    ),
    gps_conecta as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        
        from `rj-smtr`.`monitoramento`.`gps_onibus_conecta`
        where 
    
        data between date_sub(
            date('2022-01-01T00:00:00'), interval 1 day
        ) and date_add(date('2022-01-01T01:00:00'), interval 1 day)
    


    ),
    gps_zirix as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        
        from `rj-smtr`.`monitoramento`.`gps_onibus_zirix`
        where 
    
        data between date_sub(
            date('2022-01-01T00:00:00'), interval 1 day
        ) and date_add(date('2022-01-01T01:00:00'), interval 1 day)
    

    ),
    gps_cittati as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        
        from `rj-smtr`.`monitoramento`.`gps_onibus_cittati`
        where 
    
        data between date_sub(
            date('2022-01-01T00:00:00'), interval 1 day
        ) and date_add(date('2022-01-01T01:00:00'), interval 1 day)
    

    ),
    gps_brt as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        
        from `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_brt`
        where 
    
        data between date_sub(
            date('2022-01-01T00:00:00'), interval 1 day
        ) and date_add(date('2022-01-01T01:00:00'), interval 1 day)
    

    ),
    gps_union as (
        select *, 'conecta' as fornecedor
        from gps_conecta

        union all

        select *, 'zirix' as fornecedor
        from gps_zirix

        union all

        select *, 'cittati' as fornecedor
        from gps_cittati

        union all

        select *, 'brt' as fornecedor
        from gps_brt
    )
select
    v.data,
    g.timestamp_gps,
    v.modo,
    g.id_veiculo,
    v.servico as servico_viagem,
    g.servico as servico_gps,
    v.sentido,
    g.latitude,
    g.longitude,
    st_geogpoint(g.longitude, g.latitude) as geo_point_gps,
    v.id_viagem,
    v.datetime_partida,
    v.datetime_chegada,
    v.trip_id,
    v.route_id,
    v.shape_id,
    v.fonte_gps,
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from gps_union g
join
    viagem v
    on g.timestamp_gps between v.datetime_partida and v.datetime_chegada
    and g.id_veiculo = v.id_veiculo
    and g.fornecedor = v.fonte_gps
