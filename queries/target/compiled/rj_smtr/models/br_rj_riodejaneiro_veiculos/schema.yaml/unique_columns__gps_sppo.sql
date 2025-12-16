





with validation_errors as (

    select
        timestamp_gps, id_veiculo, latitude, longitude
    from (select * from `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_sppo` where DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00')) dbt_subquery
    group by timestamp_gps, id_veiculo, latitude, longitude
    having count(*) > 1

)

select *
from validation_errors


