



-- 1. Define datas do período planejado
with data_efetiva as (
    select
        data,
        tipo_dia,
        data_versao_shapes,
        data_versao_trips,
        data_versao_frequencies
    from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva`
    where data between date_sub("2022-01-01T01:00:00", interval 1 day) and date("2022-01-01T01:00:00")
),
-- 2. Puxa dados de distancia quadro no quadro horário
quadro as (
    select
        e.data,
        e.tipo_dia,
        p.* except(tipo_dia, data_versao, horario_inicio, horario_fim),
        IF(horario_inicio IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_inicio, ":")) = 3,
            DATETIME_ADD(
                DATETIME(
                    e.data,
                    PARSE_TIME("%T",
                        CONCAT(
                        SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(1)] AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(2)] AS INT64)
                        )
                    )
                ),
                INTERVAL DIV(SAFE_CAST(SPLIT(horario_inicio, ":")[OFFSET(0)] AS INT64), 24) DAY
            ),
            NULL
        ) AS inicio_periodo,
        IF(horario_fim IS NOT NULL AND ARRAY_LENGTH(SPLIT(horario_fim, ":")) = 3,
            DATETIME_ADD(
                DATETIME(
                    e.data,
                    PARSE_TIME("%T",
                        CONCAT(
                        SAFE_CAST(MOD(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(1)] AS INT64),
                        ":",
                        SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(2)] AS INT64)
                        )
                    )
                ),
                INTERVAL DIV(SAFE_CAST(SPLIT(horario_fim, ":")[OFFSET(0)] AS INT64), 24) DAY
            ),
            NULL
        ) AS fim_periodo
    from
        data_efetiva e
    inner join (
        select *
        from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_quadro_horario`
        
        where
            data_versao in (select data_versao_frequencies from data_efetiva)
        
    ) p
    on
        e.data_versao_frequencies = p.data_versao
    and
        e.tipo_dia = p.tipo_dia
),
-- 3. Trata informação de trips: adiciona ao sentido da trip o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
trips as (
    select
        e.data,
        t.*
    from (
        select *
        from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_trips_desaninhada`
        
        where
            data_versao in (select data_versao_trips from data_efetiva)
        
    ) t
    inner join
        data_efetiva e
    on
        t.data_versao = e.data_versao_trips
),
quadro_trips as (
    select
        *
    from (
        select distinct
            * except(trip_id),
            trip_id as trip_id_planejado,
            trip_id
        from
            quadro
        where sentido = "I" or sentido = "V"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(trip_id, "_0") as trip_id,
        from
            quadro
        where sentido = "C"
    )
    union all (
        select
            * except(trip_id),
            trip_id as trip_id_planejado,
            concat(trip_id, "_1") as trip_id,
        from
            quadro
        where sentido = "C"
    )
),
quadro_tratada as (
    select
        q.*,
        t.shape_id as shape_id_planejado,
        case
            when sentido = "C"
            then shape_id || "_" || split(q.trip_id, "_")[offset(1)]
            else shape_id
        end as shape_id, -- TODO: adicionar no sigmob
    from
        quadro_trips q
    left join
        trips t
    on
        t.data = q.data
    and
        t.trip_id = q.trip_id_planejado
),
-- 4. Trata informações de shapes: junta trips e shapes para resgatar o sentido
--    planejado (os shapes/trips circulares são separados em
--    ida/volta no sigmob)
shapes as (
    select
        e.data,
        data_versao as data_shape,
        shape_id,
        shape,
        start_pt,
        end_pt
    from
        data_efetiva e
    inner join (
        select *
        from `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_shapes_geom`
        
        where
            data_versao in (select data_versao_shapes from data_efetiva)
        
    ) s
    on
        s.data_versao = e.data_versao_shapes
)
-- 5. Junta shapes e trips aos servicos planejados no quadro horário
select
    p.*,
    s.data_shape,
    s.shape,
    case
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "0" then "I"
        when p.sentido = "C" and split(p.shape_id, "_")[offset(1)] = "1" then "V"
        when p.sentido = "I" or p.sentido = "V" then p.sentido
    end as sentido_shape,
    s.start_pt,
    s.end_pt,
    SAFE_CAST(NULL AS INT64) AS id_tipo_trajeto, -- Adaptação para formato da SUBSIDIO_V6
    SAFE_CAST(NULL AS STRING) AS feed_version, -- Adaptação para formato da SUBSIDIO_V6
    CURRENT_DATETIME("America/Sao_Paulo") AS datetime_ultima_atualizacao -- Adaptação para formato da SUBSIDIO_V7
from
    quadro_tratada p
inner join
    shapes s
on
    p.shape_id = s.shape_id
and
    p.data = s.data

