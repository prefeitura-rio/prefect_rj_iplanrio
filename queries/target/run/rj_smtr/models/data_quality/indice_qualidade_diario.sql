

  create or replace table `rj-smtr-dev`.`data_quality`.`indice_qualidade_diario`
  partition by data_particao
  
  OPTIONS(
      description=""""""
    )
  as (
    



SELECT 
    data_particao,
    hora_particao,
    dataset_id,
    table_id,
    CASE
        
        WHEN validade = 0
        THEN 'ruim'
        
        WHEN completude = 0
        THEN 'ruim'
        
        WHEN acuracia = 0
        THEN 'ruim'
        
        WHEN atualizacao = 0
        THEN 'ruim'
        
        WHEN exclusividade = 0
        THEN 'ruim'
        
        WHEN consistencia = 0
        THEN 'ruim'
        
    ELSE 'bom'
    end as indice_qualidade
from `rj-smtr-dev`.`data_quality`.`indicadores_view`
  );
  