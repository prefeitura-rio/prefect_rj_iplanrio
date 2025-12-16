

WITH matriz_melt AS (
  SELECT
    i.id,
    im.sequencia_integracao,
    im.id_tipo_modal,
    im.perc_rateio,
    i.dt_inicio_validade,
    i.dt_fim_validade
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`percentual_rateio_integracao` i,
    -- Transforma colunas com os dados de cada modo da integração em linhas diferentes
    UNNEST(
      [
        
          STRUCT(
            
              id_tipo_modal_origem AS id_tipo_modal,
            
              perc_rateio_origem AS perc_rateio,
            
            0 AS sequencia_integracao
          ),
        
          STRUCT(
            
              id_tipo_modal_integracao_t1 AS id_tipo_modal,
            
              perc_rateio_integracao_t1 AS perc_rateio,
            
            1 AS sequencia_integracao
          ),
        
          STRUCT(
            
              id_tipo_modal_integracao_t2 AS id_tipo_modal,
            
              perc_rateio_integracao_t2 AS perc_rateio,
            
            2 AS sequencia_integracao
          ),
        
          STRUCT(
            
              id_tipo_modal_integracao_t3 AS id_tipo_modal,
            
              perc_rateio_integracao_t3 AS perc_rateio,
            
            3 AS sequencia_integracao
          ),
        
          STRUCT(
            
              id_tipo_modal_integracao_t4 AS id_tipo_modal,
            
              perc_rateio_integracao_t4 AS perc_rateio,
            
            4 AS sequencia_integracao
          )
        
      ]
    ) im
)
SELECT
  i.id AS id_matriz_integracao,
  i.sequencia_integracao,
  m.modo,
  i.perc_rateio AS percentual_rateio,
  i.dt_inicio_validade AS data_inicio_validade,
  i.dt_fim_validade AS data_fim_validade,
  '' as versao
FROM
  matriz_melt i
LEFT JOIN
    `rj-smtr`.`cadastro`.`modos` m
ON
  i.id_tipo_modal = m.id_modo AND m.fonte = "jae"
WHERE
  i.id_tipo_modal IS NOT NULL