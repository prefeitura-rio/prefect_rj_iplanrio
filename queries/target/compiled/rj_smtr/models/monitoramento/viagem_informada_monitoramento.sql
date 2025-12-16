











    
        

        

         
        
    



with
    staging_rioonibus as (
        select
            data_viagem as data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            fornecedor as fonte_gps,
            datetime_processamento,
            timestamp_captura as datetime_captura
        from `rj-smtr`.`monitoramento_staging`.`viagem_informada_rioonibus`
         where 
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
 
    ),
    staging_brt as (
        select
            data_viagem as data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            "brt" as fonte_gps,
            datetime_processamento,
            timestamp_captura as datetime_captura
        from `rj-smtr`.`monitoramento_staging`.`viagem_informada_brt`
        where
             
  DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
 and 
            datetime_processamento >= "2024-09-10 13:00:00"
    ),
    staging_union as (
        select *
        from staging_rioonibus

        union all

        select *
        from staging_brt
    ),
    staging as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            trip_id,
            route_id,
            shape_id,
            servico,
            sentido,
            fonte_gps,
            datetime_processamento,
            datetime_captura
        from staging_union
    ),
    complete_partitions as (
        select *, 0 as priority
        from staging

        
    ),
    deduplicado as (
        select * except (rn, priority)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_viagem order by datetime_captura desc, priority
                    ) as rn
                from complete_partitions
            )
        where rn = 1
    ),
    calendario as (
        select *
        from `rj-smtr`.`planejamento`.`calendario`
        
            where data in ()
        
    ),
    routes as (
        select *
        from `rj-smtr`.`gtfs`.`routes`
        
        
            where feed_start_date in ()
        
    ),
    viagem_modo as (
        select
            data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            case
                when v.fonte_gps = 'brt'
                then 'BRT'
                when r.route_type = '200'
                then 'Ônibus Executivo'
                when r.route_type = '700'
                then 'Ônibus SPPO'
            end as modo,
            if(trim(v.id_veiculo) = '', null, v.id_veiculo) as id_veiculo,
            if(trim(v.trip_id) = '', null, v.trip_id) as trip_id,
            if(trim(v.route_id) = '', null, v.route_id) as route_id,
            if(trim(v.shape_id) = '', null, v.shape_id) as shape_id,
            if(trim(v.servico) = '', null, v.servico) as servico,
            if(trim(v.sentido) = '', null, v.sentido) as sentido,
            if(trim(v.fonte_gps) = '', null, v.fonte_gps) as fonte_gps,
            v.datetime_processamento,
            v.datetime_captura
        from deduplicado v
        join calendario c using (data)
        left join routes r using (route_id, feed_start_date, feed_version)
    )
select
    *,
    '' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_modo