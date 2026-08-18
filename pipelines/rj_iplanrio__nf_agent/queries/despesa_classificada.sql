-- =============================================================================
-- View — despesa_classificada (v2)
-- Projeto  : rj-nf-agent
-- Dataset  : brutos_cgm_poc_osinfo_ia_pipeline
-- =============================================================================
-- Lógica de indicadores baseada na view antiga poc_osinfo_ia.vw_despesas_classificada
-- (a view que rodava em produção).
--
-- Escopo: APENAS declarações cujo PDF foi processado pela pipeline de extração.
-- Ao rodar novas declarações no Prefect, a view automaticamente reflete os
-- novos resultados — sem necessidade de script de recarga.
-- =============================================================================

CREATE OR REPLACE VIEW
  `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.despesa_classificada`
OPTIONS (
  description = 'Classificação de declarações que passaram pela pipeline de extração. Indicadores como array de structs. Escopo limitado aos PDFs processados. Inclui dados externos (CNPJ, SEFAZ) via basedosdados e SMF.'
)
AS

WITH

-- ── CTE 1: Declarações base (osinfo_despesas_recorte) ──────────────────────
despesas AS (
  SELECT
    id_documento,
    cod_organizacao,
    cod_unidade,
    data_envio,
    COALESCE(cpf, cnpj)                                               AS cnpj_cpf_declaracao,
    LPAD(REGEXP_REPLACE(COALESCE(cpf, cnpj), r'[^0-9]', ''), 14, '0') AS cnpj_cpf_normalizado,
    num_documento,
    valor_documento,
    valor_pago,
    data_emissao,
    data_pagamento,
    referencia_ano,
    referencia_mes,
    REGEXP_REPLACE(
      COALESCE(descricao_limpa, descricao),
      r'(?i)\.pdf$', ''
    ) AS nome_arquivo_limpo
  FROM
    `rj-nf-agent.poc_osinfo_ia.osinfo_despesas_recorte`
),

-- ── CTE 2: APENAS declarações que passaram pela pipeline de extração ───────
documentos_processados AS (
  SELECT DISTINCT d.*
  FROM despesas d
  INNER JOIN (
    SELECT DISTINCT nome_arquivo
    FROM `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina`
  ) ep
    ON d.nome_arquivo_limpo = ep.nome_arquivo
),

-- ── CTE 3: Páginas com match no PROPRIO PDF ────────────────────────────────
match_proprio AS (
  SELECT
    ep.nome_arquivo,
    ep.pagina,
    ep.pipeline_status,
    ep.tipo_documento_classificacao,
    ep.tipo_documento_extracao,
    ep.numero_documento,
    ep.cnpj_emitente,
    ep.cnpj_destinatario,
    ep.valor_documento                                                AS valor_documento_ep,
    ep.data_emissao_documento,
    ep.data_competencia_documento,
    ep.data_servico_documento,
    ep.numero_rps,
    ep.observacao_extracao,
    ep.timestamp_geracao,
    ep.versao_pipeline,
    mid                                                               AS id_documento_match
  FROM
    `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina` ep,
    UNNEST(ep.match_id_documento) AS mid
  INNER JOIN documentos_processados d
    ON mid = d.id_documento AND ep.nome_arquivo = d.nome_arquivo_limpo
  WHERE
    ep.pipeline_status = 'ok'
),

-- ── CTE 4: Páginas com match em PDF DIFERENTE ─────────────────────────────
match_outro_pdf AS (
  SELECT DISTINCT
    mid AS id_documento_match
  FROM
    `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina` ep,
    UNNEST(ep.match_id_documento) AS mid
  INNER JOIN documentos_processados d
    ON mid = d.id_documento AND ep.nome_arquivo != d.nome_arquivo_limpo
  WHERE
    ep.pipeline_status = 'ok'
),

-- ── CTE 5: PDFs com NF extraída de fato ───────────────────────────────────
nf_extraida_por_arquivo AS (
  SELECT DISTINCT
    nome_arquivo,
    TRUE AS tem_nf_extraida
  FROM
    `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina`
  WHERE
    pipeline_status = 'ok'
    AND tipo_documento_classificacao != 'Nenhuma das Opções'
    AND COALESCE(tipo_documento_extracao, '') NOT IN ('Outros', 'Outro', '')
),

-- ── CTE 6: PDFs com erro de processamento ─────────────────────────────────
arquivos_com_erro AS (
  SELECT DISTINCT
    nome_arquivo,
    TRUE AS tem_erro_processamento
  FROM
    `rj-nf-agent.brutos_cgm_poc_osinfo_ia_pipeline.extracao_pagina`
  WHERE
    pipeline_status = 'erro_processamento'
),

-- ── CTE 7: Join principal ─────────────────────────────────────────────────
declaracoes_com_extracao AS (
  SELECT
    d.id_documento,
    d.cod_organizacao,
    d.cod_unidade,
    d.data_envio,
    d.cnpj_cpf_declaracao,
    d.cnpj_cpf_normalizado,
    d.num_documento,
    d.valor_documento,
    d.valor_pago,
    d.data_emissao,
    d.data_pagamento,
    d.referencia_ano,
    d.referencia_mes,
    d.nome_arquivo_limpo,

    mp.pagina                      AS pagina_match,
    mp.nome_arquivo                AS nome_arquivo_match,
    mp.pipeline_status             AS pipeline_status_pagina,
    mp.tipo_documento_classificacao,
    mp.tipo_documento_extracao,
    mp.numero_documento            AS numero_documento_ia,
    mp.cnpj_emitente               AS cnpj_emitente_ia,
    LPAD(REGEXP_REPLACE(COALESCE(mp.cnpj_emitente, ''), r'[^0-9]', ''), 14, '0')
                                   AS cnpj_emitente_normalizado,
    mp.cnpj_destinatario           AS cnpj_destinatario_ia,
    mp.valor_documento_ep          AS valor_documento_ia,
    mp.data_emissao_documento      AS data_emissao_documento_ia,
    mp.data_competencia_documento  AS data_competencia_documento_ia,
    mp.data_servico_documento      AS data_servico_documento_ia,
    mp.numero_rps                  AS numero_rps_ia,
    mp.observacao_extracao         AS observacao_extracao_ia,
    mp.timestamp_geracao           AS timestamp_geracao_pagina,
    mp.versao_pipeline,

    CASE WHEN mop.id_documento_match IS NOT NULL THEN TRUE ELSE FALSE END
                                   AS tem_match_outro_pdf,
    COALESCE(nfe.tem_nf_extraida, FALSE)
                                   AS pdf_tem_nf_extraida,
    COALESCE(ae.tem_erro_processamento, FALSE)
                                   AS pdf_tem_erro_processamento

  FROM
    documentos_processados d
  LEFT JOIN match_proprio mp
    ON d.id_documento = mp.id_documento_match
  LEFT JOIN match_outro_pdf mop
    ON d.id_documento = mop.id_documento_match
  LEFT JOIN nf_extraida_por_arquivo nfe
    ON d.nome_arquivo_limpo = nfe.nome_arquivo
  LEFT JOIN arquivos_com_erro ae
    ON d.nome_arquivo_limpo = ae.nome_arquivo
),

-- ── CTE 8: Dados externos — CNPJ (situação cadastral via basedosdados) ────
base_cnpj_limpa AS (
  WITH dicionario AS (
    SELECT chave, valor
    FROM `basedosdados.br_me_cnpj.dicionario`
    WHERE nome_coluna = 'situacao_cadastral' AND id_tabela = 'estabelecimentos'
  )
  SELECT
    cnpj,
    d.valor AS situacao_cadastral,
    data_situacao_cadastral
  FROM `basedosdados.br_me_cnpj.estabelecimentos` e
  LEFT JOIN dicionario d ON e.situacao_cadastral = d.chave
  WHERE data_situacao_cadastral >= '2021-01-01'
),

-- ── CTE 9: Dados externos — CNPJ (data de abertura via recorte) ───────────
bcadastro AS (
  SELECT
    LPAD(REGEXP_REPLACE(CAST(cnpj_particao AS STRING), r'[^0-9]', ''), 14, '0') AS cleaned_cnpj_reg,
    inicio_atividade_data AS cnpj_data_abertura
  FROM `rj-nf-agent.poc_osinfo_ia.bcadastro_cnpj_recorte`
),

-- ── CTE 10: Dados externos — NF cancelada/substituta (SMF) ────────────────
smf_nf_cancelada AS (
  SELECT
    nome_arquivo_declaracao,
    CAST(numero_nf_modelo AS INT64) AS numero_nf_modelo_int,
    cnpj_cpf_modelo,
    data_cancelamento,
    indicador_nf_substituta_declarada,
    indicador_nf_substituida
  FROM `rj-nf-agent.poc_osinfo_ia.smf_nf_cancelada_substituta_20260311`
),

-- ── CTE 11: Métricas de rateio/duplicidade por nota fiscal ─────────────────
metricas_por_nota AS (
  SELECT
    numero_documento_ia,
    cnpj_emitente_ia,
    COUNT(DISTINCT id_documento)  AS qtd_declaracoes_mesma_nota,
    SUM(valor_pago)               AS soma_valor_pago_mesma_nota
  FROM
    declaracoes_com_extracao
  WHERE
    numero_documento_ia  IS NOT NULL
    AND cnpj_emitente_ia IS NOT NULL
  GROUP BY
    numero_documento_ia,
    cnpj_emitente_ia
),

-- ── CTE 12: Ranking das declarações dentro de cada nota ────────────────────
rank_por_nota AS (
  SELECT
    id_documento,
    numero_documento_ia,
    cnpj_emitente_ia,
    DENSE_RANK() OVER (
      PARTITION BY numero_documento_ia, cnpj_emitente_ia
      ORDER BY data_envio, id_documento
    ) AS rank_declaracao
  FROM
    declaracoes_com_extracao
  WHERE
    numero_documento_ia  IS NOT NULL
    AND cnpj_emitente_ia IS NOT NULL
),

-- ── CTE 13: Computar todos os indicadores ──────────────────────────────────
indicadores AS (
  SELECT
    dce.*,

    COALESCE(mn.qtd_declaracoes_mesma_nota, 1)  AS qtd_declaracoes_mesma_nota,
    mn.soma_valor_pago_mesma_nota,
    rn.rank_declaracao,

    -- nf_encontrada
    CASE
      WHEN dce.pagina_match IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS nf_encontrada,

    -- motivo_nf_nao_encontrada
    CASE
      WHEN dce.pagina_match IS NOT NULL THEN CAST(NULL AS STRING)
      WHEN dce.tem_match_outro_pdf IS TRUE THEN 'match_em_pdf_errado'
      WHEN dce.pdf_tem_nf_extraida IS FALSE THEN 'pdf_sem_nf_extraida'
      WHEN dce.pdf_tem_nf_extraida IS TRUE THEN 'pdf_processado_sem_match'
      ELSE CAST(NULL AS STRING)
    END AS motivo_nf_nao_encontrada,

    -- match_valor
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.valor_documento_ia IS NULL OR dce.valor_documento IS NULL THEN NULL
      ELSE CAST(dce.valor_documento AS FLOAT64) = dce.valor_documento_ia
    END AS match_valor,

    -- match_numero_documento
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.numero_documento_ia IS NULL OR dce.num_documento IS NULL THEN NULL
      ELSE (
        REGEXP_REPLACE(dce.num_documento,       r'[^0-9]', '') =
        REGEXP_REPLACE(dce.numero_documento_ia, r'[^0-9]', '')
      )
    END AS match_numero_documento,

    -- match_cnpj
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.cnpj_emitente_ia IS NULL OR dce.cnpj_cpf_declaracao IS NULL THEN NULL
      ELSE (dce.cnpj_cpf_normalizado = dce.cnpj_emitente_normalizado)
    END AS match_cnpj,

    -- match_data_emissao
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.data_emissao_documento_ia IS NULL OR dce.data_emissao IS NULL THEN NULL
      ELSE (
        COALESCE(
          SAFE.PARSE_DATE('%Y-%m-%d', dce.data_emissao_documento_ia),
          SAFE.PARSE_DATE('%d/%m/%Y', dce.data_emissao_documento_ia)
        ) = dce.data_emissao
      )
    END AS match_data_emissao,

    -- nf_nao_duplicada
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.cod_organizacao IS NULL OR dce.cod_unidade IS NULL THEN NULL
      WHEN mn.soma_valor_pago_mesma_nota IS NULL OR dce.valor_documento_ia IS NULL THEN NULL
      WHEN rn.rank_declaracao IS NULL THEN NULL
      WHEN dce.cod_organizacao <> dce.cod_unidade
        AND mn.soma_valor_pago_mesma_nota > CAST(dce.valor_documento_ia AS NUMERIC) + 1
        AND rn.rank_declaracao > 1
        THEN FALSE
      ELSE TRUE
    END AS nf_nao_duplicada,

    -- valor_pago_menor_documento
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN mn.soma_valor_pago_mesma_nota IS NULL OR dce.valor_documento_ia IS NULL THEN NULL
      ELSE NOT (mn.soma_valor_pago_mesma_nota > CAST(dce.valor_documento_ia AS NUMERIC) + 1)
    END AS valor_pago_menor_documento,

    -- cnpj_ativo (basedosdados.br_me_cnpj)
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN e.cnpj IS NULL OR dce.cnpj_emitente_ia IS NULL THEN NULL
      WHEN dce.data_pagamento IS NULL OR dce.data_pagamento < '2021-11-01' THEN NULL
      WHEN e.situacao_cadastral = 'Ativa' THEN TRUE
      ELSE FALSE
    END AS cnpj_ativo,

    -- emissao_posterior_abertura_cnpj (recorte bcadastro)
    -- NULL quando não encontramos o CNPJ na base (não conseguimos avaliar)
    -- TRUE  quando a NF foi emitida após ou na data de abertura do CNPJ
    -- FALSE quando a NF foi emitida antes da abertura do CNPJ (apontamento)
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN dce.data_emissao_documento_ia IS NULL THEN NULL
      WHEN r.cnpj_data_abertura IS NULL THEN NULL
      WHEN DATE(r.cnpj_data_abertura) <= COALESCE(
          SAFE.PARSE_DATE('%Y-%m-%d', dce.data_emissao_documento_ia),
          SAFE.PARSE_DATE('%d/%m/%Y', dce.data_emissao_documento_ia)
        )
        THEN TRUE
      ELSE FALSE
    END AS emissao_posterior_abertura_cnpj,

    -- nf_nao_cancelada (SMF)
    CASE
      WHEN dce.pagina_match IS NULL THEN NULL
      WHEN s.numero_nf_modelo_int IS NULL THEN NULL
      WHEN s.data_cancelamento IS NULL THEN TRUE
      WHEN (s.indicador_nf_substituta_declarada = 'FALSE' OR s.indicador_nf_substituida = 'FALSE') THEN FALSE
      ELSE NULL
    END AS nf_nao_cancelada

  FROM
    declaracoes_com_extracao dce
  LEFT JOIN metricas_por_nota mn
    ON  dce.numero_documento_ia  = mn.numero_documento_ia
    AND dce.cnpj_emitente_ia     = mn.cnpj_emitente_ia
  LEFT JOIN rank_por_nota rn
    ON  dce.id_documento         = rn.id_documento
  LEFT JOIN base_cnpj_limpa e
    ON  dce.cnpj_emitente_normalizado = e.cnpj
    AND dce.data_pagamento >= e.data_situacao_cadastral
  LEFT JOIN bcadastro r
    ON  dce.cnpj_cpf_normalizado = r.cleaned_cnpj_reg
  LEFT JOIN smf_nf_cancelada s
    ON  dce.nome_arquivo_limpo = s.nome_arquivo_declaracao
    AND CAST(REGEXP_REPLACE(dce.numero_documento_ia, r'[^0-9]', '') AS INT64) = s.numero_nf_modelo_int
    AND dce.cnpj_emitente_ia = s.cnpj_cpf_modelo
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dce.id_documento
    ORDER BY e.data_situacao_cadastral DESC
  ) = 1
)

-- ── SELECT FINAL ───────────────────────────────────────────────────────────
SELECT
  id_documento,
  cod_organizacao,
  cod_unidade,
  data_envio                       AS data_envio_declaracao,
  nome_arquivo_limpo               AS nome_arquivo_declaracao,
  referencia_ano                   AS referencia_ano_declaracao,
  referencia_mes                   AS referencia_mes_declaracao,
  num_documento                    AS numero_documento_declaracao,
  cnpj_cpf_declaracao,
  valor_documento                  AS valor_documento_declaracao,
  valor_pago                       AS valor_pago_declaracao,
  data_emissao                     AS data_emissao_declaracao,
  data_pagamento                   AS data_pagamento_declaracao,

  pagina_match,
  nome_arquivo_match,
  tipo_documento_classificacao     AS tipo_documento_classificacao_ia,
  tipo_documento_extracao          AS tipo_documento_extracao_ia,
  numero_documento_ia,
  cnpj_emitente_ia,
  cnpj_destinatario_ia,
  valor_documento_ia,
  data_emissao_documento_ia,
  data_competencia_documento_ia,
  data_servico_documento_ia,
  numero_rps_ia,
  observacao_extracao_ia,

  CAST(qtd_declaracoes_mesma_nota AS INT64)  AS qtd_declaracoes_mesma_nota,
  CAST(soma_valor_pago_mesma_nota AS NUMERIC) AS soma_valor_pago_mesma_nota,
  rank_declaracao,

  -- Array de indicadores (sem pode_ser_null)
  [
    STRUCT(
      'nf_encontrada' AS nome,
      CAST(nf_encontrada AS BOOL) AS valor,
      CASE
        WHEN nf_encontrada IS NULL THEN CAST(NULL AS STRING)
        WHEN nf_encontrada = TRUE THEN CAST(NULL AS STRING)
        ELSE
          CASE motivo_nf_nao_encontrada
            WHEN 'match_em_pdf_errado'    THEN 'NF encontrada em PDF diferente do referenciado pela declaração'
            WHEN 'pdf_sem_nf_extraida'    THEN 'Nenhuma nota fiscal foi extraída do PDF referenciado'
            WHEN 'pdf_processado_sem_match' THEN 'Nenhuma correspondência com as NFs extraídas do PDF referenciado'
            ELSE 'NF não encontrada'
          END
      END AS motivo
    ),
    STRUCT(
      'match_valor' AS nome,
      CAST(match_valor AS BOOL) AS valor,
      CASE
        WHEN match_valor IS NULL OR match_valor = TRUE THEN CAST(NULL AS STRING)
        ELSE CONCAT(
          'Valor declarado (R$ ', CAST(CAST(valor_documento AS FLOAT64) AS STRING),
          ') não corresponde ao valor extraído pela IA (R$ ', CAST(valor_documento_ia AS STRING), ')'
        )
      END AS motivo
    ),
    STRUCT(
      'nf_nao_duplicada' AS nome,
      CAST(nf_nao_duplicada AS BOOL) AS valor,
      CASE
        WHEN nf_nao_duplicada IS NULL OR nf_nao_duplicada = TRUE THEN CAST(NULL AS STRING)
        ELSE CONCAT(
          'NF duplicada. Soma paga: R$ ', CAST(ROUND(CAST(soma_valor_pago_mesma_nota AS FLOAT64), 2) AS STRING),
          ', valor NF: R$ ', CAST(ROUND(valor_documento_ia, 2) AS STRING),
          '. Rank: ', CAST(rank_declaracao AS STRING)
        )
      END AS motivo
    ),
    STRUCT(
      'valor_pago_menor_documento' AS nome,
      CAST(valor_pago_menor_documento AS BOOL) AS valor,
      CASE
        WHEN valor_pago_menor_documento IS NULL OR valor_pago_menor_documento = TRUE THEN CAST(NULL AS STRING)
        ELSE CONCAT(
          'Soma dos valores pagos (R$ ', CAST(ROUND(CAST(soma_valor_pago_mesma_nota AS FLOAT64), 2) AS STRING),
          ') excede o valor extraído da nota pela IA (R$ ', CAST(ROUND(valor_documento_ia, 2) AS STRING), ')'
        )
      END AS motivo
    ),
    STRUCT(
      'cnpj_ativo' AS nome,
      CAST(cnpj_ativo AS BOOL) AS valor,
      CASE
        WHEN cnpj_ativo IS NULL OR cnpj_ativo = TRUE THEN CAST(NULL AS STRING)
        ELSE CONCAT('CNPJ ', cnpj_emitente_ia, ' não estava ativo na data do pagamento')
      END AS motivo
    ),
    STRUCT(
      'emissao_posterior_abertura_cnpj' AS nome,
      CAST(emissao_posterior_abertura_cnpj AS BOOL) AS valor,
      CASE
        WHEN emissao_posterior_abertura_cnpj IS NULL OR emissao_posterior_abertura_cnpj = TRUE THEN CAST(NULL AS STRING)
        ELSE CONCAT(
          'NF emitida em ', COALESCE(data_emissao_documento_ia, 'data desconhecida'),
          ' antes da abertura do CNPJ'
        )
      END AS motivo
    ),
    STRUCT(
      'nf_nao_cancelada' AS nome,
      CAST(nf_nao_cancelada AS BOOL) AS valor,
      CASE
        WHEN nf_nao_cancelada IS NULL OR nf_nao_cancelada = TRUE THEN CAST(NULL AS STRING)
        ELSE 'NF consta como cancelada no sistema SMF'
      END AS motivo
    )
  ] AS indicadores,

  -- Classificação final (inclui indicadores externos)
  CASE
    WHEN pdf_tem_erro_processamento IS TRUE
      THEN 'Não avaliado — erro de processamento'
    WHEN nf_encontrada = FALSE AND motivo_nf_nao_encontrada = 'pdf_processado_sem_match'
      THEN 'Sem match'
    WHEN nf_encontrada = FALSE
      THEN 'Apontamento Grave'
    WHEN nf_encontrada = TRUE AND (
      valor_pago_menor_documento = FALSE
      OR nf_nao_duplicada = FALSE
      OR cnpj_ativo = FALSE
      OR emissao_posterior_abertura_cnpj = FALSE
      OR nf_nao_cancelada = FALSE
    )
      THEN 'Apontamento Grave'
    WHEN nf_encontrada = TRUE AND match_valor = FALSE
      THEN 'Apontamento Leve'
    WHEN nf_encontrada = TRUE
      THEN 'Sem apontamentos'
    ELSE 'Não avaliado — erro de processamento'
  END AS classificacao_modelo,

  motivo_nf_nao_encontrada,
  timestamp_geracao_pagina,
  versao_pipeline,
  CURRENT_TIMESTAMP()              AS timestamp_consulta

FROM indicadores;