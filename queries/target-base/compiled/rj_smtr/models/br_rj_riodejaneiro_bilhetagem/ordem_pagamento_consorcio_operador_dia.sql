
-- depends_on: `rj-smtr`.`br_rj_riodejaneiro_bilhetagem`.`ordem_pagamento_servico_operador_dia`




  
    -- Verifica as ordens de pagamento capturadas
    

    
  


WITH pagamento AS (
  SELECT
    data_pagamento,
    data_ordem,
    id_consorcio,
    id_operadora,
    valor_pago
  FROM
    `rj-smtr`.`controle_financeiro_staging`.`aux_retorno_ordem_pagamento`
    -- `rj-smtr.controle_financeiro_staging.aux_retorno_ordem_pagamento`
  
    WHERE
    
      data_ordem = '2000-01-01'
    
  
),
ordem_pagamento AS (
  SELECT
  o.data_ordem,
  o.id_ordem_pagamento_consorcio_operadora AS id_ordem_pagamento_consorcio_operador_dia,
  dc.id_consorcio,
  dc.consorcio,
  do.id_operadora,
  do.operadora,
  op.id_ordem_pagamento AS id_ordem_pagamento,
  o.qtd_debito AS quantidade_transacao_debito,
  o.valor_debito,
  o.qtd_vendaabordo AS quantidade_transacao_especie,
  o.valor_vendaabordo AS valor_especie,
  o.qtd_gratuidade AS quantidade_transacao_gratuidade,
  o.valor_gratuidade,
  o.qtd_integracao AS quantidade_transacao_integracao,
  o.valor_integracao,
  o.qtd_rateio_credito AS quantidade_transacao_rateio_credito,
  o.valor_rateio_credito AS valor_rateio_credito,
  o.qtd_rateio_debito AS quantidade_transacao_rateio_debito,
  o.valor_rateio_debito AS valor_rateio_debito,
  (
    o.qtd_debito
    + o.qtd_vendaabordo
    + o.qtd_gratuidade
    + o.qtd_integracao
  ) AS quantidade_total_transacao,
  o.valor_bruto AS valor_total_transacao_bruto,
  o.valor_taxa AS valor_desconto_taxa,
  o.valor_liquido AS valor_total_transacao_liquido_ordem
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_pagamento_consorcio_operadora` o
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.ordem_pagamento_consorcio_operadora` o
  JOIN
    `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`ordem_pagamento` op
    -- `rj-smtr.br_rj_riodejaneiro_bilhetagem_staging.ordem_pagamento` op
  ON
    o.data_ordem = op.data_ordem
  LEFT JOIN
    `rj-smtr`.`cadastro`.`operadoras` do
    -- `rj-smtr.cadastro.operadoras` do
  ON
    o.id_operadora = do.id_operadora_jae
  LEFT JOIN
    `rj-smtr`.`cadastro`.`consorcios` dc
    -- `rj-smtr.cadastro.consorcios` dc
  ON
    o.id_consorcio = dc.id_consorcio_jae
  
    WHERE
      DATE(o.data) BETWEEN DATE("2022-01-01T00:00:00") AND DATE("2022-01-01T01:00:00")
  
),
ordem_pagamento_completa AS (
  SELECT
    *,
    0 AS priority
  FROM
    ordem_pagamento

  
),
ordem_valor_pagamento AS (
  SELECT
    data_ordem,
    id_ordem_pagamento_consorcio_operador_dia,
    id_consorcio,
    o.consorcio,
    id_operadora,
    o.operadora,
    o.id_ordem_pagamento,
    o.quantidade_transacao_debito,
    o.valor_debito,
    o.quantidade_transacao_especie,
    o.valor_especie,
    o.quantidade_transacao_gratuidade,
    o.valor_gratuidade,
    o.quantidade_transacao_integracao,
    o.valor_integracao,
    o.quantidade_transacao_rateio_credito,
    o.valor_rateio_credito,
    o.quantidade_transacao_rateio_debito,
    o.valor_rateio_debito,
    o.quantidade_total_transacao,
    o.valor_total_transacao_bruto,
    o.valor_desconto_taxa,
    o.valor_total_transacao_liquido_ordem,
    p.data_pagamento,
    p.valor_pago,
    ROW_NUMBER() OVER (PARTITION BY data_ordem, id_consorcio, id_operadora ORDER BY priority) AS rn
  FROM
    ordem_pagamento_completa o
  LEFT JOIN
    pagamento p
  USING(data_ordem, id_consorcio, id_operadora)
)
SELECT
  data_ordem,
  id_ordem_pagamento_consorcio_operador_dia,
  id_consorcio,
  consorcio,
  id_operadora,
  operadora,
  id_ordem_pagamento,
  quantidade_transacao_debito,
  valor_debito,
  quantidade_transacao_especie,
  valor_especie,
  quantidade_transacao_gratuidade,
  valor_gratuidade,
  quantidade_transacao_integracao,
  valor_integracao,
  quantidade_transacao_rateio_credito,
  valor_rateio_credito,
  quantidade_transacao_rateio_debito,
  valor_rateio_debito,
  quantidade_total_transacao,
  valor_total_transacao_bruto,
  valor_desconto_taxa,
  valor_total_transacao_liquido_ordem,
  CASE
    WHEN
      data_ordem = '2024-06-07'
      AND id_consorcio = '2'
      AND id_operadora = '8'
    THEN
      valor_total_transacao_liquido_ordem - 1403.4532 -- Corrigir valor pago incorretamente ao VLT na ordem do dia 2024-05-31
    ELSE valor_total_transacao_liquido_ordem
  END AS valor_total_transacao_liquido,
  data_pagamento,
  valor_pago,
  '' AS versao
FROM
  ordem_valor_pagamento
WHERE
  rn = 1