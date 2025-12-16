

select data, id_veiculo, count(*) as quantidade_gps
from `rj-smtr`.`br_rj_riodejaneiro_veiculos`.`gps_sppo`

where
    data > '2025-03-31'
    
        and data between date("2022-01-01T00:00:00") and date(
            "2022-01-01T01:00:00"
        )
    
group by 1, 2