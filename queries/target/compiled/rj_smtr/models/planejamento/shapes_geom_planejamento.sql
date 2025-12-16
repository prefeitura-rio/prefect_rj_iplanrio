

-- depends_on: `rj-smtr`.`gtfs`.`feed_info`

    


with
    shapes as (
        select
            feed_version,
            feed_start_date,
            feed_end_date,
            shape_id,
            shape_pt_sequence,
            st_geogpoint(shape_pt_lon, shape_pt_lat) as ponto_shape,
            concat(shape_pt_lon, " ", shape_pt_lat) as lon_lat,
        from `rj-smtr`.`gtfs`.`shapes`
        
        
            where
                feed_start_date
                in ('2024-12-23', '2024-12-31')
        
    ),
    shapes_agg as (
        select
            feed_start_date,
            feed_end_date,
            feed_version,
            shape_id,
            array_agg(ponto_shape order by shape_pt_sequence) as array_shape,
            concat(
                "LINESTRING(", string_agg(lon_lat, ", " order by shape_pt_sequence), ")"
            ) as wkt_shape

        from shapes
        group by 1, 2, 3, 4
    )
select
    feed_start_date,
    feed_end_date,
    feed_version,
    shape_id,
    st_makeline(array_shape) as shape,
    wkt_shape,
    array_shape[ordinal(1)] as start_pt,
    array_shape[ordinal(array_length(array_shape))] as end_pt,
    '' as versao
from shapes_agg