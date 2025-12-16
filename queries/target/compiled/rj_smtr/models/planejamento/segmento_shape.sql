

-- depends_on: `rj-smtr`.`gtfs`.`feed_info`

    


with
    aux_segmento as (
        select
            feed_start_date,
            feed_end_date,
            feed_version,
            shape_id,
            id_segmento,
            st_geogfromtext(wkt_segmento) as segmento,
            wkt_segmento,
            round(cast(comprimento_segmento as float64), 2) as comprimento_segmento,
            st_geogfromtext(buffer_completo) as buffer_completo
        from `rj-smtr`.`planejamento_staging`.`aux_segmento_shape`
    ),
    tunel as (
        select
            inicio_vigencia as inicio_vigencia_tunel,
            fim_vigencia as fim_vigencia_tunel,
            st_union_agg(
                st_buffer(geometry, 50)
            ) as buffer_tunel
        from `rj-smtr`.`planejamento`.`tuneis`
        where
            (date('2024-12-23') between inicio_vigencia and fim_vigencia)
            or (
                date('2024-12-23') >= inicio_vigencia
                and fim_vigencia is null
            )
            or (date('2024-12-23') <= inicio_vigencia)
        group by 1, 2
    ),
    intercessao_segmento as (
        select
            s1.shape_id,
            s1.id_segmento,
            st_union(array_agg(s2.buffer_completo)) as buffer_segmento_posterior
        from aux_segmento s1
        join
            aux_segmento s2
            on s1.shape_id = s2.shape_id
            and s1.id_segmento < s2.id_segmento
            and st_intersects(s1.buffer_completo, s2.buffer_completo)
        group by 1, 2
    ),
    buffer_segmento_recortado as (
        select
            s.*,
            coalesce(
                st_difference(buffer_completo, i.buffer_segmento_posterior),
                buffer_completo
            ) as buffer
        from aux_segmento s
        left join intercessao_segmento i using (shape_id, id_segmento)
    ),
    indicador_validacao_shape as (
        select
            s.*,
            t.inicio_vigencia_tunel,
            t.fim_vigencia_tunel,
            coalesce(
                st_intersects(s.segmento, t.buffer_tunel), false
            ) as indicador_tunel,
            st_area(s.buffer) / st_area(s.buffer_completo)
            < 0.5 as indicador_area_prejudicada,
            s.comprimento_segmento
            < 990
            as indicador_segmento_pequeno,
            cast(id_segmento as integer) as id_segmento_int
        from buffer_segmento_recortado s
        left join tunel t on st_intersects(s.segmento, t.buffer_tunel)
    )
select
    * except (id_segmento_int),
    (
        (
            indicador_tunel
            and (
                (id_segmento_int > 1)
                and (
                    id_segmento_int
                    < max(id_segmento_int) over (partition by feed_start_date, shape_id)
                )
            )
        )
        or indicador_area_prejudicada
        or indicador_segmento_pequeno
    ) as indicador_segmento_desconsiderado,
    '' as versao
from indicador_validacao_shape



    union all

    select
        s.feed_start_date,
        fi.feed_end_date,
        s.feed_version,
        s.shape_id,
        s.id_segmento,
        s.segmento,
        s.wkt_segmento,
        s.comprimento_segmento,
        s.buffer_completo,
        s.buffer,
        s.inicio_vigencia_tunel,
        s.fim_vigencia_tunel,
        s.indicador_tunel,
        s.indicador_area_prejudicada,
        s.indicador_segmento_pequeno,
        s.indicador_segmento_desconsiderado,
        s.versao
    from `rj-smtr`.`planejamento`.`segmento_shape` s
    join `rj-smtr`.`gtfs`.`feed_info` fi using (feed_start_date)
    
    where feed_start_date = '2024-12-23'

