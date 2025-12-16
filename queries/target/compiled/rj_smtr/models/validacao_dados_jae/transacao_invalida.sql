


  

    

    
    

    
    
  


WITH transacao AS (
  SELECT
    t.data,
    t.hora,
    t.datetime_transacao,
    t.datetime_processamento,
    t.datetime_captura,
    t.modo,
    t.id_consorcio,
    t.consorcio,
    t.id_operadora,
    t.operadora,
    t.id_servico_jae,
    t.servico_jae,
    t.descricao_servico_jae,
    t.id_transacao,
    t.longitude,
    t.latitude,
    IFNULL(t.longitude, 0) AS longitude_tratada,
    IFNULL(t.latitude, 0) AS latitude_tratada,
    s.longitude AS longitude_servico,
    s.latitude AS latitude_servico,
    s.id_servico_gtfs,
    s.id_servico_jae AS id_servico_jae_cadastro
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao` t
  LEFT JOIN
    `rj-smtr`.`cadastro`.`servicos` s
  ON
    t.id_servico_jae = s.id_servico_jae
    AND t.data >= s.data_inicio_vigencia AND (t.data <= s.data_fim_vigencia OR s.data_fim_vigencia IS NULL)
    
      WHERE
      
        data = "2000-01-01"
      
    
),
indicadores AS (
  SELECT
    * EXCEPT(id_servico_gtfs, latitude_tratada, longitude_tratada, id_servico_jae_cadastro),
    latitude_tratada = 0 OR longitude_tratada = 0 AS indicador_geolocalizacao_zerada,
    (
      (latitude_tratada != 0 OR longitude_tratada != 0)
      AND NOT ST_INTERSECTSBOX(ST_GEOGPOINT(longitude_tratada, latitude_tratada), -43.87, -23.13, -43.0, -22.59)
    ) AS indicador_geolocalizacao_fora_rio,
    (
      latitude_tratada != 0
      AND longitude_tratada != 0
      AND latitude_servico IS NOT NULL
      AND longitude_servico IS NOT NULL
      AND modo = "BRT"
      AND ST_DISTANCE(ST_GEOGPOINT(longitude_tratada, latitude_tratada), ST_GEOGPOINT(longitude_servico, latitude_servico)) > 100
    ) AS indicador_geolocalizacao_fora_stop,
    id_servico_gtfs IS NULL AND id_servico_jae_cadastro IS NOT NULL AND modo IN ("Ônibus", "BRT") AS indicador_servico_fora_gtfs,
    id_servico_jae_cadastro IS NULL AS indicador_servico_fora_vigencia
  FROM
    transacao
)
SELECT
  * EXCEPT(indicador_servico_fora_gtfs, indicador_servico_fora_vigencia),
  CASE
    WHEN indicador_geolocalizacao_zerada = TRUE THEN "Geolocalização zerada"
    WHEN indicador_geolocalizacao_fora_rio = TRUE THEN "Geolocalização fora do município"
    WHEN indicador_geolocalizacao_fora_stop = TRUE THEN "Geolocalização fora do stop"
  END AS descricao_geolocalizacao_invalida,
  indicador_servico_fora_gtfs,
  indicador_servico_fora_vigencia,
  '' as versao
FROM
  indicadores
WHERE
  indicador_geolocalizacao_zerada = TRUE
  OR indicador_geolocalizacao_fora_rio = TRUE
  OR indicador_geolocalizacao_fora_stop = TRUE
  OR indicador_servico_fora_gtfs = TRUE
  OR indicador_servico_fora_vigencia = TRUE