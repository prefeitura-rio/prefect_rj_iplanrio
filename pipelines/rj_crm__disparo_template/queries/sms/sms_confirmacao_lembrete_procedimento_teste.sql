-- TESTE - SMS - Confirmação / lembrete de procedimento (Supercentro)
-- Pega as 3 primeiras linhas (ordenadas por id_agendamento) da query de produção
-- (sms_confirmacao_lembrete_procedimento.sql) e troca cpf, telefone e nome pelos contatos de teste.
-- Os demais campos (procedimento, data, horário, unidade, ids) são dos agendamentos reais.
WITH
  producao AS (
    SELECT
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone) AS telefone,
      procedimento AS tipo_atendimento,
      data,
      hora AS horario,
      CONCAT(local, " - ", endereco) AS unidade,
      id_agendamento,
      id_solicitacao,
      CONCAT("no ", local) AS no_equipamento,
      dias_para_atendimento AS janela_confirmacao
    FROM `rj-sms.projeto_whatsapp.disparo_whatsapp_supercentro_20261127`
    WHERE
      cpf IS NOT NULL
      AND `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(telefone) IS NOT NULL
  ),

  primeiras_linhas AS (
    SELECT
      * EXCEPT (telefone),
      ROW_NUMBER() OVER (ORDER BY id_agendamento) AS linha
    FROM producao
    QUALIFY ROW_NUMBER() OVER (ORDER BY id_agendamento) <= 3
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT(1 AS linha, '5511984677798' AS telefone, '00000000350' AS cpf, 'patricia' AS nome),
      STRUCT(2 AS linha, '5521980375732' AS telefone, '00000000349' AS cpf, 'rodolpho' AS nome),
      STRUCT(3 AS linha, '5521989190512' AS telefone, '00000000032' AS cpf, 'maria salesforce' AS nome)
    ])
  )

SELECT
  t.cpf,
  t.telefone,
  `rj-crm-registry.udf.FORMAT_NAME`(t.nome, TRUE) AS nome_sobrenome,
  p.tipo_atendimento,
  p.data,
  p.horario,
  p.unidade,
  p.id_agendamento,
  p.id_solicitacao,
  p.no_equipamento,
  p.janela_confirmacao
FROM primeiras_linhas AS p
INNER JOIN contatos_teste AS t
  ON t.linha = p.linha
