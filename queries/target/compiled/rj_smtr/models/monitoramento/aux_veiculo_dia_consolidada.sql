

select data, id_veiculo, placa, modo, tecnologia, status, indicadores
from `rj-smtr`.`veiculo`.`sppo_veiculo_dia`

union all

select data, id_veiculo, placa, modo, tecnologia, status, indicadores
from `rj-smtr`.`monitoramento`.`veiculo_dia`
where modo is null or (modo = 'ONIBUS' and tipo_veiculo not like '%ROD%')