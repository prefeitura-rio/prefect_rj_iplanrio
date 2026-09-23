-- scoring: corte = agora, ou seja, as features usam todo o histórico disponível.
-- DATETIME em America/Sao_Paulo (não TIMESTAMP): é o tipo de envio_datahora e de
-- registro_data_atualizacao, então o corte precisa ser do mesmo tipo pra comparar.
--
-- `alvo` = pares (cpf, telefone) que um disparo pode de fato escolher entre telefones:
--   - CPF vivo no mestre (pessoa_fisica);
--   - telefone com estrategia_envio != 'NÃO ENVIAR' (mesmo critério de elegibilidade de
--     eventos_candidatos);
--   - CPF com 2 ou mais telefones elegíveis (com 1 só não há o que escolher).
-- Sem esses filtros seriam ~144M de pares (todo CPF/telefone de qualquer cadastro, 99%
-- sem nenhum disparo); com eles, ~4,4M de pares e ~3,4M de telefones. Só o universo a
-- pontuar é filtrado: as features de cada telefone continuam calculadas sobre TODAS as
-- aparições (`aparicoes`), sem filtro.
pares_elegiveis AS (
  SELECT DISTINCT LPAD(ap.proprietario_id, 11, '0') AS cpf, ap.telefone
  FROM aparicoes ap
  JOIN `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
    ON pf.cpf = LPAD(ap.proprietario_id, 11, '0')
  WHERE ap.proprietario_tipo = 'CPF'
    AND ap.proprietario_id IS NOT NULL
    AND pf.obito.indicador IS FALSE
    AND ap.telefone IN (
      SELECT telefone_numero_completo
      FROM `rj-crm-registry.intermediario_rmi_telefones.int_telefone`
      WHERE estrategia_envio IS NOT NULL AND estrategia_envio != 'NÃO ENVIAR'
    )
),
alvo AS (
  SELECT cpf, telefone
  FROM pares_elegiveis
  QUALIFY COUNT(*) OVER (PARTITION BY cpf) >= 2
),
amostra AS (
  SELECT DISTINCT telefone, CURRENT_DATETIME('America/Sao_Paulo') AS data_corte
  FROM alvo
),
