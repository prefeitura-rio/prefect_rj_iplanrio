





with
    gratuidade_complete_partitions as (
        select
            cast(cast(cd_cliente as float64) as int64) as id_cliente,
            id as id_gratuidade,
            tipo_gratuidade,
            data_inclusao as data_inicio_validade,
            timestamp_captura
        from `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`gratuidade`
    ),
    gratuidade_deduplicada as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_gratuidade, id_cliente
                        order by timestamp_captura desc
                    ) as rn
                from gratuidade_complete_partitions
            )
        where rn = 1
    )
select
    concat(id_cliente, '_', id_gratuidade) as id_cliente_gratuidade,
    id_cliente,
    id_gratuidade,
    tipo_gratuidade,
    data_inicio_validade,
    lead(data_inicio_validade) over (
        partition by id_cliente order by data_inicio_validade
    ) as data_fim_validade,
    timestamp_captura
from gratuidade_deduplicada