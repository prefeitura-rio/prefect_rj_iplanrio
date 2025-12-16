
  
  
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  
    
  


SELECT
  *
FROM
  `rj-smtr`.`catalogo`.`metadado_coluna`
WHERE
  
    (dataset_id = "br_rj_riodejaneiro_bilhetagem"
    AND table_id = "gps_validador")
  
    OR (dataset_id = "br_rj_riodejaneiro_bilhetagem"
    AND table_id = "gps_validador_van")
  
    OR (dataset_id = "br_rj_riodejaneiro_bilhetagem_staging"
    AND table_id = "cliente")
  
    OR (dataset_id = "planejamento"
    AND table_id = "segmento_shape")
  
    OR (dataset_id = "planejamento"
    AND table_id = "shapes_geom")
  
    OR (dataset_id = "br_rj_riodejaneiro_veiculos"
    AND table_id = "gps_sppo_15_minutos")
  
    OR (dataset_id = "br_rj_riodejaneiro_veiculos"
    AND table_id = "gps_sppo")
  
    OR (dataset_id = "br_rj_riodejaneiro_veiculos"
    AND table_id = "gps_brt")
  
    OR (dataset_id = "br_rj_riodejaneiro_onibus_gps_zirix"
    AND table_id = "gps_sppo")
  
    OR (dataset_id = "monitoramento"
    AND table_id = "gps_15_minutos_onibus_conecta")
  
    OR (dataset_id = "monitoramento"
    AND table_id = "gps_onibus_conecta")
  
    OR (dataset_id = "gtfs"
    AND table_id = "shapes")
  
    OR (dataset_id = "gtfs"
    AND table_id = "stops")
  
    OR (dataset_id = "gtfs"
    AND table_id = "shapes_geom")
  
    OR (dataset_id = "cadastro"
    AND table_id = "servicos")
  
    OR (dataset_id = "cadastro"
    AND table_id = "operadoras")
  

  OR (dataset_id = "br_rj_riodejaneiro_stpl_gps"
    AND table_id = "registros")