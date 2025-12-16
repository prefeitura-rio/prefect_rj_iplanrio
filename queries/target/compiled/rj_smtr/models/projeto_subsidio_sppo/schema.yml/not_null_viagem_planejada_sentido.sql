
    
    



select sentido
from (select * from `rj-smtr`.`projeto_subsidio_sppo`.`viagem_planejada` where DATA BETWEEN DATE('2022-01-01T00:00:00') AND DATE('2022-01-01T01:00:00'))
where sentido is null


