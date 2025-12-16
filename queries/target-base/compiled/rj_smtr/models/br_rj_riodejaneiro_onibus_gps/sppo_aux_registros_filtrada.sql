



  /*
Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros que estão fora de uma caixa que contém a área do município de Rio de Janeiro.
2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.
3. Muda o nome de variáveis para o padrão do projeto.
	- id_veiculo --> ordem
*/
WITH
box AS (
  /*1. Geometria de caixa que contém a área do município de Rio de Janeiro.*/
	SELECT
	*
	FROM
	rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa
),
gps AS (
  /*2. Filtra registros antigos. Remove registros que tem diferença maior que 1 minuto entre o timestamp_captura e timestamp_gps.*/
  SELECT
    *,
    ST_GEOGPOINT(longitude, latitude) posicao_veiculo_geo
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_registros`
  WHERE
    data between DATE("2022-01-01T00:00:00") and DATE("2022-01-01T01:00:00")
  AND timestamp_gps > "2022-01-01T00:00:00" and timestamp_gps <="2022-01-01T01:00:00"),
realocacao as (
  SELECT
    g.* except(linha),
    coalesce(r.servico_realocado, g.linha) as linha
  FROM
    gps g
  LEFT JOIN
    `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_realocacao` r
  ON
    g.ordem = r.id_veiculo
    and g.timestamp_gps = r.timestamp_gps
),
filtrada AS (
  /*1,2, e 3. Muda o nome de variáveis para o padrão do projeto.*/
  SELECT
    ordem AS id_veiculo,
    latitude,
    longitude,
    posicao_veiculo_geo,
    velocidade,
    linha,
    timestamp_gps,
    timestamp_captura,
    data,
    hora,
    row_number() over (partition by ordem, timestamp_gps, linha) rn
  FROM
    realocacao
  WHERE
    ST_INTERSECTSBOX(posicao_veiculo_geo,
      ( SELECT min_longitude FROM box),
      ( SELECT min_latitude FROM box),
      ( SELECT max_longitude FROM box),
      ( SELECT max_latitude FROM box))
  )
SELECT
  * except(rn),
  "" as versao
FROM
  filtrada
WHERE
  rn = 1