



  

    

    
    

    
    
  


 -- Número máximo de pernas em uma integração



WITH matriz AS (
  SELECT
    STRING_AGG(modo order by sequencia_integracao) AS sequencia_valida
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`matriz_integracao`
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.matriz_integracao`
  group by id_matriz_integracao
),
transacao AS (
  SELECT
    id_cliente,
    
      
        datetime_transacao,
      
    
      
        id_transacao,
      
    
      
        modo,
      
    
      
        CONCAT(id_servico_jae, '_', sentido) AS servico_sentido,
      
    
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`transacao`
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
  WHERE
    data < CURRENT_DATE("America/Sao_Paulo")
    AND tipo_transacao != "Gratuidade"
    
      AND
      
  
    data = "2000-01-01"
  

    
),
transacao_agrupada AS (
  SELECT
    id_cliente,
    -- Cria o conjunto de colunas para a transação atual e as 4 próximas transações do cliente
    
      
        
          datetime_transacao AS datetime_transacao_0,
        
      
        
          LEAD(datetime_transacao, 1) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS datetime_transacao_1,
        
      
        
          LEAD(datetime_transacao, 2) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS datetime_transacao_2,
        
      
        
          LEAD(datetime_transacao, 3) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS datetime_transacao_3,
        
      
        
          LEAD(datetime_transacao, 4) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS datetime_transacao_4,
        
      
    
      
        
          id_transacao AS id_transacao_0,
        
      
        
          LEAD(id_transacao, 1) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS id_transacao_1,
        
      
        
          LEAD(id_transacao, 2) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS id_transacao_2,
        
      
        
          LEAD(id_transacao, 3) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS id_transacao_3,
        
      
        
          LEAD(id_transacao, 4) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS id_transacao_4,
        
      
    
      
        
          modo AS modo_0,
        
      
        
          LEAD(modo, 1) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS modo_1,
        
      
        
          LEAD(modo, 2) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS modo_2,
        
      
        
          LEAD(modo, 3) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS modo_3,
        
      
        
          LEAD(modo, 4) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS modo_4,
        
      
    
      
        
          servico_sentido AS servico_sentido_0,
        
      
        
          LEAD(servico_sentido, 1) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS servico_sentido_1,
        
      
        
          LEAD(servico_sentido, 2) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS servico_sentido_2,
        
      
        
          LEAD(servico_sentido, 3) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS servico_sentido_3,
        
      
        
          LEAD(servico_sentido, 4) OVER (PARTITION BY id_cliente ORDER BY datetime_transacao) AS servico_sentido_4,
        
      
    
  FROM
    transacao
),
integracao_possivel AS (
  SELECT
    *,
    
    
    
      
      (
        DATETIME_DIFF(datetime_transacao_1, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT(modo_0, ',', modo_1) IN (SELECT sequencia_valida FROM matriz)
        
          AND servico_sentido_1 != servico_sentido_0
        
      ) AS indicador_integracao_1,
      

    
      
      (
        DATETIME_DIFF(datetime_transacao_2, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT(modo_0, ',', modo_1, ',', modo_2) IN (SELECT sequencia_valida FROM matriz)
        
          AND servico_sentido_2 NOT IN (servico_sentido_0, ',', servico_sentido_1)
        
      ) AS indicador_integracao_2,
      

    
      
      (
        DATETIME_DIFF(datetime_transacao_3, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT(modo_0, ',', modo_1, ',', modo_2, ',', modo_3) IN (SELECT sequencia_valida FROM matriz)
        
          AND servico_sentido_3 NOT IN (servico_sentido_0, ',', servico_sentido_1, ',', servico_sentido_2)
        
      ) AS indicador_integracao_3,
      

    
      
      (
        DATETIME_DIFF(datetime_transacao_4, datetime_transacao_0, MINUTE) <= 180
        AND CONCAT(modo_0, ',', modo_1, ',', modo_2, ',', modo_3, ',', modo_4) IN (SELECT sequencia_valida FROM matriz)
        
          AND servico_sentido_4 NOT IN (servico_sentido_0, ',', servico_sentido_1, ',', servico_sentido_2, ',', servico_sentido_3)
        
      ) AS indicador_integracao_4,
      

    
  FROM
    transacao_agrupada
  WHERE
    id_transacao_1 IS NOT NULL
),
transacao_filtrada AS (
  SELECT
    id_cliente,
    
      
        
          datetime_transacao_0,
        
      
        
          datetime_transacao_1,
        
      
        
          CASE
            WHEN
              indicador_integracao_2 THEN datetime_transacao_2
            END AS datetime_transacao_2,
        
      
        
          CASE
            WHEN
              indicador_integracao_3 THEN datetime_transacao_3
            END AS datetime_transacao_3,
        
      
        
          CASE
            WHEN
              indicador_integracao_4 THEN datetime_transacao_4
            END AS datetime_transacao_4,
        
      
    
      
        
          id_transacao_0,
        
      
        
          id_transacao_1,
        
      
        
          CASE
            WHEN
              indicador_integracao_2 THEN id_transacao_2
            END AS id_transacao_2,
        
      
        
          CASE
            WHEN
              indicador_integracao_3 THEN id_transacao_3
            END AS id_transacao_3,
        
      
        
          CASE
            WHEN
              indicador_integracao_4 THEN id_transacao_4
            END AS id_transacao_4,
        
      
    
      
        
          modo_0,
        
      
        
          modo_1,
        
      
        
          CASE
            WHEN
              indicador_integracao_2 THEN modo_2
            END AS modo_2,
        
      
        
          CASE
            WHEN
              indicador_integracao_3 THEN modo_3
            END AS modo_3,
        
      
        
          CASE
            WHEN
              indicador_integracao_4 THEN modo_4
            END AS modo_4,
        
      
    
      
        
          servico_sentido_0,
        
      
        
          servico_sentido_1,
        
      
        
          CASE
            WHEN
              indicador_integracao_2 THEN servico_sentido_2
            END AS servico_sentido_2,
        
      
        
          CASE
            WHEN
              indicador_integracao_3 THEN servico_sentido_3
            END AS servico_sentido_3,
        
      
        
          CASE
            WHEN
              indicador_integracao_4 THEN servico_sentido_4
            END AS servico_sentido_4,
        
      
    
    indicador_integracao_1,
    indicador_integracao_2,
    indicador_integracao_3,
    indicador_integracao_4
  FROM
    integracao_possivel
  WHERE
    indicador_integracao_1
),
transacao_listada AS (
  SELECT
    *,
    ARRAY_TO_STRING(
      [
        
          id_transacao_1 ,
        
          id_transacao_2 ,
        
          id_transacao_3 ,
        
          id_transacao_4 
        
      ],
      ", "
    ) AS transacoes
  FROM
    transacao_filtrada
),

  validacao_integracao_5_pernas AS (
    SELECT
      (
        id_transacao_4 IS NOT NULL
        
        AND id_transacao_0 IN UNNEST(
          SPLIT(
            STRING_AGG(transacoes, ", ") OVER (PARTITION BY id_cliente ORDER BY datetime_transacao_0 ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
            ', ')
        )
      ) AS remover_5,
      *
    FROM
      
        transacao_listada
      
  ),

  validacao_integracao_4_pernas AS (
    SELECT
      (
        id_transacao_3 IS NOT NULL
        
          AND id_transacao_4 IS NULL
        
        AND id_transacao_0 IN UNNEST(
          SPLIT(
            STRING_AGG(transacoes, ", ") OVER (PARTITION BY id_cliente ORDER BY datetime_transacao_0 ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
            ', ')
        )
      ) AS remover_4,
      *
    FROM
      
        validacao_integracao_5_pernas
        WHERE
          NOT remover_5
      
  ),

  validacao_integracao_3_pernas AS (
    SELECT
      (
        id_transacao_2 IS NOT NULL
        
          AND id_transacao_3 IS NULL
        
        AND id_transacao_0 IN UNNEST(
          SPLIT(
            STRING_AGG(transacoes, ", ") OVER (PARTITION BY id_cliente ORDER BY datetime_transacao_0 ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
            ', ')
        )
      ) AS remover_3,
      *
    FROM
      
        validacao_integracao_4_pernas
        WHERE
          NOT remover_4
      
  ),

  validacao_integracao_2_pernas AS (
    SELECT
      (
        id_transacao_1 IS NOT NULL
        
          AND id_transacao_2 IS NULL
        
        AND id_transacao_0 IN UNNEST(
          SPLIT(
            STRING_AGG(transacoes, ", ") OVER (PARTITION BY id_cliente ORDER BY datetime_transacao_0 ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING),
            ', ')
        )
      ) AS remover_2,
      *
    FROM
      
        validacao_integracao_3_pernas
        WHERE
          NOT remover_3
      
  ),

integracoes_validas AS (
  SELECT
    DATE(datetime_transacao_0) AS data,
    id_transacao_0 AS id_integracao,
    *
  FROM
    validacao_integracao_2_pernas
  WHERE
    NOT remover_2
),
melted AS (
  SELECT
    data,
    id_integracao,
    sequencia_integracao,
    datetime_transacao,
    id_transacao,
    modo,
    SPLIT(servico_sentido, '_')[0] AS id_servico_jae,
    SPLIT(servico_sentido, '_')[1] AS sentido,
    COUNTIF(modo = "BRT") OVER(PARTITION BY id_integracao) > 1 AS indicador_transferencia
  FROM
    integracoes_validas,
    UNNEST(
      [
        
          STRUCT(
            
              datetime_transacao_0 AS datetime_transacao,
            
              id_transacao_0 AS id_transacao,
            
              modo_0 AS modo,
            
              servico_sentido_0 AS servico_sentido,
            
            1 AS sequencia_integracao
          ),
      
          STRUCT(
            
              datetime_transacao_1 AS datetime_transacao,
            
              id_transacao_1 AS id_transacao,
            
              modo_1 AS modo,
            
              servico_sentido_1 AS servico_sentido,
            
            2 AS sequencia_integracao
          ),
      
          STRUCT(
            
              datetime_transacao_2 AS datetime_transacao,
            
              id_transacao_2 AS id_transacao,
            
              modo_2 AS modo,
            
              servico_sentido_2 AS servico_sentido,
            
            3 AS sequencia_integracao
          ),
      
          STRUCT(
            
              datetime_transacao_3 AS datetime_transacao,
            
              id_transacao_3 AS id_transacao,
            
              modo_3 AS modo,
            
              servico_sentido_3 AS servico_sentido,
            
            4 AS sequencia_integracao
          ),
      
          STRUCT(
            
              datetime_transacao_4 AS datetime_transacao,
            
              id_transacao_4 AS id_transacao,
            
              modo_4 AS modo,
            
              servico_sentido_4 AS servico_sentido,
            
            5 AS sequencia_integracao
          )
      
      ]
    )
),
integracao_nao_realizada AS (
  SELECT DISTINCT
    id_integracao
  FROM
    melted
  WHERE
    NOT indicador_transferencia
    AND id_transacao NOT IN (
      SELECT
        id_transacao
      FROM
        `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`integracao`
        -- `rj-smtr.br_rj_riodejaneiro_bilhetagem.integracao`
      
        WHERE
          
  
    data = "2000-01-01"
  

      
    )
)
SELECT
  * EXCEPT(indicador_transferencia),
  '' as versao
FROM
  melted
WHERE
  id_integracao IN (SELECT id_integracao FROM integracao_nao_realizada)
  AND id_transacao IS NOT NULL