

WITH
  feed_start_date AS (
  SELECT
    feed_start_date,
    feed_start_date AS data_inicio,
    COALESCE(DATE_SUB(LEAD(feed_start_date) OVER (ORDER BY feed_start_date), INTERVAL 1 DAY), LAST_DAY(feed_start_date, MONTH)) AS data_fim
  FROM (
    SELECT
      DISTINCT feed_start_date,
    FROM
      `rj-smtr`.`gtfs`.`ordem_servico` )),
  ordem_servico_pivot AS (
  SELECT
    *
  FROM
    `rj-smtr`.`gtfs`.`ordem_servico`
    PIVOT( MAX(partidas_ida) AS partidas_ida,
      MAX(partidas_volta) AS partidas_volta,
      MAX(viagens_planejadas) AS viagens_planejadas,
      MAX(distancia_total_planejada) AS km FOR
      tipo_dia IN (
        'Dia Útil' AS du,
        'Ponto Facultativo' AS pf,
        'Sabado' AS sab,
        'Domingo' AS dom ))),
  subsidio_feed_start_date_efetiva AS (
  SELECT
    data,
    SPLIT(tipo_dia, " - ")[0] AS tipo_dia,
    tipo_dia AS tipo_dia_original
  FROM
    `rj-smtr`.`projeto_subsidio_sppo`.`subsidio_data_versao_efetiva` )
SELECT
  DATA,
  tipo_dia_original AS tipo_dia,
  servico,
  vista,
  consorcio,
  sentido,
  CASE
  
  
      WHEN sentido  IN ('I', 'C')  AND tipo_dia = "Dia Útil" THEN  partidas_ida_du 
    
      WHEN sentido  IN ('I', 'C')  AND tipo_dia = "Ponto Facultativo" THEN  partidas_ida_pf 
    
      WHEN sentido  IN ('I', 'C')  AND tipo_dia = "Sabado" THEN  ROUND(SAFE_DIVIDE((partidas_ida_du * km_sab), km_du)) 
    
      WHEN sentido  IN ('I', 'C')  AND tipo_dia = "Domingo" THEN  ROUND(SAFE_DIVIDE((partidas_ida_du * km_dom), km_du)) 
    
      WHEN sentido  = "V"  AND tipo_dia = "Dia Útil" THEN  partidas_volta_du 
    
      WHEN sentido  = "V"  AND tipo_dia = "Ponto Facultativo" THEN  partidas_volta_pf 
    
      WHEN sentido  = "V"  AND tipo_dia = "Sabado" THEN  ROUND(SAFE_DIVIDE((partidas_volta_du * km_sab), km_du)) 
    
      WHEN sentido  = "V"  AND tipo_dia = "Domingo" THEN  ROUND(SAFE_DIVIDE((partidas_volta_du * km_dom), km_du)) 
    END
  AS viagens_planejadas,
  horario_inicio AS inicio_periodo,
  horario_fim AS fim_periodo
FROM
  UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(data_inicio) FROM feed_start_date), (SELECT MAX(data_fim) FROM feed_start_date))) AS DATA
LEFT JOIN
  feed_start_date AS d
ON
  DATA BETWEEN d.data_inicio
  AND d.data_fim
LEFT JOIN
  subsidio_feed_start_date_efetiva AS sd
USING
  (DATA)
LEFT JOIN
  ordem_servico_pivot AS o
USING
  (feed_start_date)
LEFT JOIN
  `rj-smtr`.`gtfs`.`servicos_sentido`
USING
  (feed_start_date,
    servico)