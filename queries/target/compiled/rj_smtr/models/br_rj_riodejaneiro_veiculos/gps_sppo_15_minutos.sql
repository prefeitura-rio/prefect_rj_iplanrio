
/*
Descrição:
Junção dos passos de tratamento, junta as informações extras que definimos a partir dos registros
capturados.
Para descrição detalhada de como cada coluna é calculada, consulte a documentação de cada uma das tabelas
utilizadas abaixo.
1. registros_filtrada: filtragem e tratamento básico dos dados brutos capturados.
2. aux_registros_velocidade: estimativa da velocidade de veículo a cada ponto registrado e identificação
do estado de movimento ('parado', 'andando')
3. aux_registros_parada: identifica veículos parados em terminais ou garagens conhecidas
4. aux_registros_flag_trajeto_correto: calcula intersecções das posições registradas para cada veículo
com o traçado da linha informada.
5. As junções (joins) são feitas sobre o id_veículo e a timestamp_gps.
*/
WITH
   __dbt__cte__sppo_aux_registros_velocidade as (

/*
Descrição:
Estimativa das velocidades dos veículos nos últimos 10 minutos contados a partir da timestamp_gps atual.
Essa metodologia serve para determinar quais carros estão em movimento e quais estão parados.
1. Calculamos a velocidade do veículo no último trecho de 10 minutos de operação.
A implementação utiliza a função 'first_value' com uma janela (cláusula 'over') de até 10 minutos anteriores à
timestamp_gps atual e calcula a distância do ponto mais antigo (o first_value na janela) ao ponto atual (posicao_veiculo_geo).
Dividimos essa distância pela diferença de tempo entre a timestamp_gps atual e a timestamp_gps do ponto mais
antigo da janela (o qual recuperamos novamente com o uso de first_value).
Esta diferença de tempo (datetime_diff) é calculada em segundos, portanto multiplicamos o resultado da divisão por um fator
3.6 para que a velocidade esteja em quilômetros por hora. O resultado final é arrendondado sem casas decimais.
Por fim, cobrimos esse cálculo com a função 'if_null' e retornamos zero para a velocidade em casos onde a divisão retornaria
um valor nulo.
2. Após o calculo da velocidade, definimos a coluna 'status_movimento'. Veículos abaixo da 'velocidade_limiar_parado', são
considerados como 'parado'. Caso contrário, são considerados 'andando'
*/
with
    t_velocidade as (
    select
        data,
        id_veiculo,
        timestamp_gps,
        linha,
        ST_DISTANCE(
                posicao_veiculo_geo,
                lag(posicao_veiculo_geo) over (
                partition by id_veiculo, linha
                order by timestamp_gps)
        ) distancia,
        IFNULL(
            SAFE_DIVIDE(
                ST_DISTANCE(
                posicao_veiculo_geo,
                lag(posicao_veiculo_geo) over (
                partition by id_veiculo, linha
                order by timestamp_gps)
                ),
                DATETIME_DIFF(
                timestamp_gps,
                lag(timestamp_gps) over (
                partition by id_veiculo, linha
                order by timestamp_gps),
                SECOND
                )),
            0
        ) * 3.6 velocidade
    FROM  `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada`
    WHERE
        data between DATE("2022-01-01T00:00:00") and DATE("2022-01-01T01:00:00")
    AND timestamp_gps > "2022-01-01T00:00:00" and timestamp_gps <="2022-01-01T01:00:00"),
    medias as (
        select
        data,
        id_veiculo,
        timestamp_gps,
        linha,
        distancia,
        velocidade, # velocidade do pontual
        AVG(velocidade) OVER (
            PARTITION BY id_veiculo, linha
            ORDER BY unix_seconds(timestamp(timestamp_gps))
            RANGE BETWEEN 600 PRECEDING AND CURRENT ROW
        ) velocidade_media # velocidade com média móvel
    from t_velocidade
    )
SELECT
    timestamp_gps,
    data,
    id_veiculo,
    linha,
    distancia,
    ROUND(
        CASE WHEN velocidade_media > 60
            THEN 60
            ELSE velocidade_media
        END,
        1) as velocidade,
    -- 2. Determinação do estado de movimento do veículo.
    case
        when velocidade_media < 3 then false
        else true
    end flag_em_movimento,
FROM medias
),  __dbt__cte__sppo_aux_registros_parada as (


/*
Descrição:
Identifica veículos parados em terminais ou garagens conhecidas.
1. Selecionamos os terminais conhecidos e uma geometria do tipo polígono (Polygon) que contém buracos nas
localizações das garagens.
2. Calculamos as distâncias do veículos em relação aos terminais conhecidos. Definimos aqui a coluna 'nrow',
que identifica qual o terminal que está mais próximo do ponto informado. No passo final, recuperamos apenas
os dados com nrow = 1 (menor distância em relação à posição do veículo)
3. Definimos uma distancia_limiar_parada. Caso o veículo esteja a uma distância menor que este valor de uma
parada, será considerado como parado no terminal com menor distancia.
4. Caso o veiculo não esteja intersectando o polígono das garagens, ele será considerado como parado dentro
de uma garagem (o polígono é vazado nas garagens, a não intersecção implica em estar dentro de um dos 'buracos').
*/
WITH
  terminais as (
    -- 1. Selecionamos terminais, criando uma geometria de ponto para cada.
    select
      ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_terminal nome_parada, 'terminal' tipo_parada
    from rj-smtr.br_rj_riodejaneiro_transporte.terminais_onibus_coordenadas
  ),
  garagem_polygon AS (
    -- 1. Selecionamos o polígono das garagens.
    SELECT  ST_GEOGFROMTEXT(WKT,make_valid => true) AS poly
    FROM rj-smtr.br_rj_riodejaneiro_geo.garagens_polygon
  ),
  distancia AS (
    --2. Calculamos as distâncias e definimos nrow
    SELECT
      id_veiculo,
      timestamp_gps,
      data,
      linha,
      posicao_veiculo_geo,
      nome_parada,
      tipo_parada,
      ROUND(ST_DISTANCE(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
      ROW_NUMBER() OVER (PARTITION BY timestamp_gps, id_veiculo, linha ORDER BY ST_DISTANCE(posicao_veiculo_geo, ponto_parada)) nrow
    FROM terminais p
    JOIN (
      SELECT
        id_veiculo,
        timestamp_gps,
        data,
        linha,
        posicao_veiculo_geo
      FROM
        `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada`
      
      WHERE
        data between DATE("2022-01-01T00:00:00") and DATE("2022-01-01T01:00:00")
      AND timestamp_gps > "2022-01-01T00:00:00" and timestamp_gps <="2022-01-01T01:00:00"
      
  ) r
    on 1=1
  )
SELECT
  data,
  id_veiculo,
  timestamp_gps,
  linha,
  /*
  3. e 4. Identificamos o status do veículo como 'terminal', 'garagem' (para os veículos parados) ou
  null (para os veículos mais distantes de uma parada que o limiar definido)
  */
  case
    when distancia_parada < 250 then tipo_parada
    when not ST_INTERSECTS(posicao_veiculo_geo, (SELECT  poly FROM garagem_polygon)) then 'garagem'
    else null
  end tipo_parada,
FROM distancia
WHERE nrow = 1
),  __dbt__cte__sppo_aux_registros_flag_trajeto_correto as (


/*
Descrição:
Calcula se o veículo está dentro do trajeto correto dado o traçado (shape) cadastrado no SIGMOB em relação à linha que está sendo
transmitida.
1. Calcula as intersecções definindo um 'buffer', utilizado por st_dwithin para identificar se o ponto está à uma
distância menor ou igual ao tamanho do buffer em relação ao traçado definido no SIGMOB.
2. Calcula um histórico de intersecções nos ultimos 10 minutos de registros de cada carro. Definimos que o carro é
considerado fora do trajeto definido se a cada 10 minutos, ele não esteve dentro do traçado planejado pelo menos uma
vez.
3. Identifica se a linha informada no registro capturado existe nas definições presentes no SIGMOB.
4. Definimos em outra tabela uma 'data_versao_efetiva', esse passo serve tanto para definir qual versão do SIGMOB utilizaremos em
caso de falha na captura, quanto para definir qual versão será utilizada para o cálculo retroativo do histórico de registros que temos.
5. Como não conseguimos identificar o itinerário que o carro está realizando, no passo counts, os resultados de
intersecções são dobrados, devido ao fato de cada linha apresentar dois itinerários possíveis (ida/volta). Portanto,
ao final, realizamos uma agregação LOGICAL_OR que é true caso o carro esteja dentro do traçado de algum dos itinerários
possíveis para a linha informada.
*/
WITH
  registros AS (
    SELECT
      id_veiculo,
      linha,
      latitude,
      longitude,
      data,
      posicao_veiculo_geo,
      timestamp_gps
    FROM
      `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada` r
    WHERE
      data between DATE("2022-01-01T00:00:00") and DATE("2022-01-01T01:00:00")
    AND timestamp_gps > "2022-01-01T00:00:00" and timestamp_gps <="2022-01-01T01:00:00"),
  intersec AS (
    SELECT
      r.*,
      s.data_versao,
      s.linha_gtfs,
      s.route_id,
      -- 1. Buffer e intersecções
      CASE
        WHEN st_dwithin(shape, posicao_veiculo_geo, 500) THEN TRUE
        ELSE FALSE
      END AS flag_trajeto_correto,
      -- 2. Histórico de intersecções nos últimos 10 minutos a partir da timestamp_gps atual
      CASE
        WHEN
          COUNT(CASE WHEN st_dwithin(shape, posicao_veiculo_geo, 500) THEN 1 END)
          OVER (PARTITION BY id_veiculo
                ORDER BY UNIX_SECONDS(TIMESTAMP(timestamp_gps))
                RANGE BETWEEN 600 PRECEDING AND CURRENT ROW) >= 1
          THEN True
        ELSE False
      END AS flag_trajeto_correto_hist,
      -- 3. Identificação de cadastro da linha no SIGMOB
      CASE WHEN s.linha_gtfs IS NULL THEN False ELSE True END AS flag_linha_existe_sigmob,
    -- 4. Join com data_versao_efetiva para definição de quais shapes serão considerados no cálculo das flags
    FROM registros r
    LEFT JOIN (
      SELECT *
      FROM `rj-smtr`.`br_rj_riodejaneiro_sigmob`.`shapes_geom`
      WHERE id_modal_smtr in ('22', '23', 'O')
      AND data_versao = "2022-06-10"
    ) s
    ON
      r.linha = s.linha_gtfs
  )
    -- 5. Agregação com LOGICAL_OR para evitar duplicação de registros
    SELECT
      id_veiculo,
      linha,
      linha_gtfs,
      route_id,
      data,
      timestamp_gps,
      LOGICAL_OR(flag_trajeto_correto) AS flag_trajeto_correto,
      LOGICAL_OR(flag_trajeto_correto_hist) AS flag_trajeto_correto_hist,
      LOGICAL_OR(flag_linha_existe_sigmob) AS flag_linha_existe_sigmob,
    FROM intersec i
    GROUP BY
      id_veiculo,
      linha,
      linha_gtfs,
      route_id,
      data,
      data_versao,
      timestamp_gps
), registros as (
  -- 1. registros_filtrada
    SELECT
      id_veiculo,
      timestamp_gps,
      timestamp_captura,
      velocidade,
      linha,
      latitude,
      longitude,
    FROM `rj-smtr`.`br_rj_riodejaneiro_onibus_gps`.`sppo_aux_registros_filtrada`
    WHERE
      data = DATE("2022-01-01T01:00:00")
      AND timestamp_gps > DATETIME_SUB("2022-01-01T01:00:00", INTERVAL 75 MINUTE)
      AND timestamp_gps <= "2022-01-01T01:00:00"
  ),
  velocidades AS (
    -- 2. velocidades
    SELECT
      id_veiculo, timestamp_gps, linha, velocidade, distancia, flag_em_movimento
    FROM
      __dbt__cte__sppo_aux_registros_velocidade
  ),
  paradas as (
    -- 3. paradas
    SELECT
      id_veiculo, timestamp_gps, linha, tipo_parada,
    FROM __dbt__cte__sppo_aux_registros_parada
  ),
  flags AS (
    -- 4. flag_trajeto_correto
    SELECT
      id_veiculo,
      timestamp_gps,
      linha,
      route_id,
      flag_linha_existe_sigmob,
      flag_trajeto_correto,
      flag_trajeto_correto_hist
    FROM
      __dbt__cte__sppo_aux_registros_flag_trajeto_correto
  )
-- 5. Junção final
SELECT
  "SPPO" modo,
  r.timestamp_gps,
  date(r.timestamp_gps) data,
  extract(time from r.timestamp_gps) hora,
  r.id_veiculo,
  r.linha as servico,
  r.latitude,
  r.longitude,
  CASE
    WHEN
      flag_em_movimento IS true AND flag_trajeto_correto_hist is true
      THEN true
  ELSE false
  END flag_em_operacao,
  v.flag_em_movimento,
  p.tipo_parada,
  flag_linha_existe_sigmob,
  flag_trajeto_correto,
  flag_trajeto_correto_hist,
  CASE
    WHEN flag_em_movimento IS true AND flag_trajeto_correto_hist is true
    THEN 'Em operação'
    WHEN flag_em_movimento is true and flag_trajeto_correto_hist is false
    THEN 'Operando fora trajeto'
    WHEN flag_em_movimento is false
    THEN
        CASE
            WHEN tipo_parada is not null
            THEN concat("Parado ", tipo_parada)
        ELSE
            CASE
                WHEN flag_trajeto_correto_hist is true
                THEN 'Parado trajeto correto'
            ELSE 'Parado fora trajeto'
            END
        END
    END status,
  r.velocidade velocidade_instantanea,
  v.velocidade velocidade_estimada_10_min,
  v.distancia,
  "" as versao
FROM
  registros r

JOIN
  flags f
ON
  r.id_veiculo = f.id_veiculo
  AND r.timestamp_gps = f.timestamp_gps
  AND r.linha = f.linha

JOIN
  velocidades v
ON
  r.id_veiculo = v.id_veiculo
  AND  r.timestamp_gps = v.timestamp_gps
  AND  r.linha = v.linha

JOIN
  paradas p
ON
  r.id_veiculo = p.id_veiculo
  AND  r.timestamp_gps = p.timestamp_gps
  AND r.linha = p.linha
WHERE
  DATE(r.timestamp_gps) = DATE("2022-01-01T01:00:00")
  AND r.timestamp_gps > DATETIME_SUB("2022-01-01T01:00:00", INTERVAL 75 MINUTE)
  AND r.timestamp_gps <= "2022-01-01T01:00:00"