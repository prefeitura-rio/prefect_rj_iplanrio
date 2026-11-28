-- TESTE - SMTR - Sistema Rio - Próxima etapa na região
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_sistema_rio_proxima_etapa.sql), enviados só para os contatos de teste.
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        'Zona Oeste' AS regiao,
        'Guaratiba, Santa Cruz, Campo Grande e Bangu, Realengo' AS bairros,
        'primeiro trimestre de 2027' AS previsao,
        '654' AS frota_atual,
        '1.006' AS frota_nova
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 1 AS prioridade),
      STRUCT('5521980375732' AS telefone, '00000000349' AS cpf, 1 AS prioridade),
      STRUCT('5521989190512' AS telefone, '00000000032' AS cpf, 1 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  cfg.regiao,
  cfg.bairros,
  cfg.previsao,
  cfg.frota_atual,
  cfg.frota_nova
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
