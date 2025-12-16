

declare m as (
    select max(data) as max_partition_date 
    from `rj-smtr-dev`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada`
)
SELECT
    id_veiculo
FROM
    `rj-smtr-dev`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada`
where data = m
AND
    id_veiculo is null
