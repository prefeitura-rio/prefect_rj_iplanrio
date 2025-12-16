-- depends_on: `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`matriz_integracao`


WITH integracao_transacao_deduplicada AS (
  SELECT
    * EXCEPT(rn)
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_captura DESC) AS rn
    FROM
      `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`integracao_transacao`
    WHERE
        DATE(data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
        AND timestamp_captura BETWEEN DATETIME("2022-01-01T00:00:00") AND DATETIME("2022-01-01T01:00:00")
  )
  WHERE
    rn = 1
),
integracao_melt AS (
    SELECT
      EXTRACT(DATE FROM im.data_transacao) AS data,
      EXTRACT(HOUR FROM im.data_transacao) AS hora,
      i.data_inclusao AS datetime_inclusao,
      i.data_processamento AS datetime_processamento_integracao,
      i.timestamp_captura AS datetime_captura,
      i.id AS id_integracao,
      im.sequencia_integracao,
      im.data_transacao AS datetime_transacao,
      im.id_tipo_modal,
      im.id_consorcio,
      im.id_operadora,
      im.id_linha,
      im.id_transacao,
      im.sentido,
      im.perc_rateio,
      im.valor_rateio_compensacao,
      im.valor_rateio,
      im.valor_transacao,
      i.valor_transacao_total,
      i.tx_adicional AS texto_adicional
    FROM
      integracao_transacao_deduplicada i,
      -- Transforma colunas com os dados de cada transação da integração em linhas diferentes
      UNNEST(
        [
          
            STRUCT(
              
                
                  data_transacao_t0 AS data_transacao,
                
              
                
              
                
                  id_consorcio_t0 AS id_consorcio,
                
              
                
              
                
                  id_linha_t0 AS id_linha,
                
              
                
              
                
                  id_operadora_t0 AS id_operadora,
                
              
                
              
                
              
                
              
                
                  id_tipo_modal_t0 AS id_tipo_modal,
                
              
                
                  id_transacao_t0 AS id_transacao,
                
              
                
              
                
              
                
              
                
                  perc_rateio_t0 AS perc_rateio,
                
              
                
              
                
                  sentido_t0 AS sentido,
                
              
                
                  valor_rateio_compensacao_t0 AS valor_rateio_compensacao,
                
              
                
                  valor_rateio_t0 AS valor_rateio,
                
              
                
                  valor_tarifa_t0 AS valor_tarifa,
                
              
                
                  valor_transacao_t0 AS valor_transacao,
                
              
                
              
              1 AS sequencia_integracao
            ),
          
            STRUCT(
              
                
                  data_transacao_t1 AS data_transacao,
                
              
                
              
                
                  id_consorcio_t1 AS id_consorcio,
                
              
                
              
                
                  id_linha_t1 AS id_linha,
                
              
                
              
                
                  id_operadora_t1 AS id_operadora,
                
              
                
              
                
              
                
              
                
                  id_tipo_modal_t1 AS id_tipo_modal,
                
              
                
                  id_transacao_t1 AS id_transacao,
                
              
                
              
                
              
                
              
                
                  perc_rateio_t1 AS perc_rateio,
                
              
                
              
                
                  sentido_t1 AS sentido,
                
              
                
                  valor_rateio_compensacao_t1 AS valor_rateio_compensacao,
                
              
                
                  valor_rateio_t1 AS valor_rateio,
                
              
                
                  valor_tarifa_t1 AS valor_tarifa,
                
              
                
                  valor_transacao_t1 AS valor_transacao,
                
              
                
              
              2 AS sequencia_integracao
            ),
          
            STRUCT(
              
                
                  data_transacao_t2 AS data_transacao,
                
              
                
              
                
                  id_consorcio_t2 AS id_consorcio,
                
              
                
              
                
                  id_linha_t2 AS id_linha,
                
              
                
              
                
                  id_operadora_t2 AS id_operadora,
                
              
                
              
                
              
                
              
                
                  id_tipo_modal_t2 AS id_tipo_modal,
                
              
                
                  id_transacao_t2 AS id_transacao,
                
              
                
              
                
              
                
              
                
                  perc_rateio_t2 AS perc_rateio,
                
              
                
              
                
                  sentido_t2 AS sentido,
                
              
                
                  valor_rateio_compensacao_t2 AS valor_rateio_compensacao,
                
              
                
                  valor_rateio_t2 AS valor_rateio,
                
              
                
                  valor_tarifa_t2 AS valor_tarifa,
                
              
                
                  valor_transacao_t2 AS valor_transacao,
                
              
                
              
              3 AS sequencia_integracao
            ),
          
            STRUCT(
              
                
                  data_transacao_t3 AS data_transacao,
                
              
                
              
                
                  id_consorcio_t3 AS id_consorcio,
                
              
                
              
                
                  id_linha_t3 AS id_linha,
                
              
                
              
                
                  id_operadora_t3 AS id_operadora,
                
              
                
              
                
              
                
              
                
                  id_tipo_modal_t3 AS id_tipo_modal,
                
              
                
                  id_transacao_t3 AS id_transacao,
                
              
                
              
                
              
                
              
                
                  perc_rateio_t3 AS perc_rateio,
                
              
                
              
                
                  sentido_t3 AS sentido,
                
              
                
                  valor_rateio_compensacao_t3 AS valor_rateio_compensacao,
                
              
                
                  valor_rateio_t3 AS valor_rateio,
                
              
                
                  valor_tarifa_t3 AS valor_tarifa,
                
              
                
                  valor_transacao_t3 AS valor_transacao,
                
              
                
              
              4 AS sequencia_integracao
            ),
          
            STRUCT(
              
                
                  data_transacao_t4 AS data_transacao,
                
              
                
              
                
                  id_consorcio_t4 AS id_consorcio,
                
              
                
              
                
                  id_linha_t4 AS id_linha,
                
              
                
              
                
                  id_operadora_t4 AS id_operadora,
                
              
                
              
                
              
                
              
                
                  id_tipo_modal_t4 AS id_tipo_modal,
                
              
                
                  id_transacao_t4 AS id_transacao,
                
              
                
              
                
              
                
              
                
                  perc_rateio_t4 AS perc_rateio,
                
              
                
              
                
                  sentido_t4 AS sentido,
                
              
                
                  valor_rateio_compensacao_t4 AS valor_rateio_compensacao,
                
              
                
                  valor_rateio_t4 AS valor_rateio,
                
              
                
                  valor_tarifa_t4 AS valor_tarifa,
                
              
                
                  valor_transacao_t4 AS valor_transacao,
                
              
                
              
              5 AS sequencia_integracao
            )
          
        ]
      ) AS im
),
integracao_rn AS (
  SELECT
    i.data,
    i.hora,
    i.datetime_processamento_integracao,
    i.datetime_captura,
    i.datetime_transacao,
    TIMESTAMP_DIFF(
      i.datetime_transacao,
      LAG(i.datetime_transacao) OVER(PARTITION BY i.id_integracao ORDER BY sequencia_integracao),
      MINUTE
    ) AS intervalo_integracao,
    i.id_integracao,
    i.sequencia_integracao,
    m.modo,
    dc.id_consorcio,
    dc.consorcio,
    do.id_operadora,
    do.operadora,
    i.id_linha AS id_servico_jae,
    -- s.servico,
    l.nr_linha AS servico_jae,
    l.nm_linha AS descricao_servico_jae,
    i.id_transacao,
    i.sentido,
    i.perc_rateio AS percentual_rateio,
    i.valor_rateio_compensacao,
    i.valor_rateio,
    i.valor_transacao,
    i.valor_transacao_total,
    i.texto_adicional,
    '' as versao,
    ROW_NUMBER() OVER (PARTITION BY id_transacao ORDER BY datetime_processamento_integracao DESC) AS rn
  FROM
    integracao_melt i
  LEFT JOIN
    `rj-smtr`.`cadastro`.`modos` m
  ON
    i.id_tipo_modal = m.id_modo AND m.fonte = "jae"
  LEFT JOIN
    `rj-smtr`.`cadastro`.`operadoras` AS do
  ON
    i.id_operadora = do.id_operadora_jae
  LEFT JOIN
    `rj-smtr`.`cadastro`.`consorcios` AS dc
  ON
    i.id_consorcio = dc.id_consorcio_jae
  LEFT JOIN
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha` AS l
  ON
      i.id_linha = l.cd_linha
  -- LEFT JOIN
  --   `rj-smtr`.`cadastro`.`servicos` AS s
  -- ON
  --   i.id_linha = s.id_servico_jae
  WHERE i.id_transacao IS NOT NULL
),
integracoes_teste_invalidas AS (
  SELECT DISTINCT
    i.id_integracao
  FROM
    integracao_rn i
  LEFT JOIN
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`linha_sem_ressarcimento` l
  ON
    i.id_servico_jae = l.id_linha
  WHERE
    l.id_linha IS NOT NULL
    OR i.data < "2023-07-17"
)
SELECT
  * EXCEPT(rn)
FROM
  integracao_rn
WHERE rn = 1
AND id_integracao NOT IN (SELECT id_integracao FROM integracoes_teste_invalidas)