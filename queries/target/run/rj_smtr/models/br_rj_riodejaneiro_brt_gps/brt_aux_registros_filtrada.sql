-- back compat for old kwarg name
  
  
        
    

    

    merge into `rj-smtr-dev`.`br_rj_riodejaneiro_brt_gps`.`brt_aux_registros_filtrada` as DBT_INTERNAL_DEST
        using (
/*
- Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros antigos. Remove registros que tem diferença maior
   que 1 minuto entre o timestamp_captura e timestamp_gps.
2. Filtra registros que estão fora de uma caixa que contém a área do
   município de Rio de Janeiro.
*/
WITH
box AS (
  /*1. Geometria de caixa que contém a área do município de Rio de Janeiro.*/ 
	SELECT
	*
	FROM
	rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa),
gps AS (
  /* 1. Filtra registros antigos. Remove registros que tem diferença maior
   que 1 minuto entre o timestamp_captura e timestamp_gps.*/
  SELECT
    *,
    ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
  FROM
    `rj-smtr-dev`.`br_rj_riodejaneiro_brt_gps`.`brt_registros_desaninhada`
  WHERE
    data between DATE("2023-12-14T08:00:00") and DATE("2024-04-25T12:00:00")
    AND timestamp_gps > "2023-12-14T08:00:00" and timestamp_gps <= "2024-04-25T12:00:00"
    AND DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) BETWEEN 0 AND 1
    ),
filtrada AS (
  /* 2. Filtra registros que estão fora de uma caixa que contém a área do
   município de Rio de Janeiro.*/
  SELECT
    id_veiculo,
    latitude,
    longitude,
    posicao_veiculo_geo,
    velocidade,
    servico,
    timestamp_gps,
    timestamp_captura,
    data,
    hora,
    row_number() over (partition by id_veiculo, timestamp_gps, servico) rn
  FROM
    gps
  WHERE
    ST_INTERSECTSBOX(posicao_veiculo_geo,
      ( SELECT min_longitude FROM box),
      ( SELECT min_latitude FROM box),
      ( SELECT max_longitude FROM box),
      ( SELECT max_latitude FROM box)) 
  )
SELECT
  * except(rn),
  "4fff566868277e25927d1aaeea35bef4ce259d9f" as versao
FROM
  filtrada
WHERE
  rn = 1
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`id_veiculo`, `latitude`, `longitude`, `posicao_veiculo_geo`, `velocidade`, `servico`, `timestamp_gps`, `timestamp_captura`, `data`, `hora`, `versao`)
    values
        (`id_veiculo`, `latitude`, `longitude`, `posicao_veiculo_geo`, `velocidade`, `servico`, `timestamp_gps`, `timestamp_captura`, `data`, `hora`, `versao`)


    