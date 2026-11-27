-- TESTE - SMTR - Terminais: resumo regional (Template 5)
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_novos_terminais.sql), enviados só para os contatos de teste.
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        'Zona Norte' AS regiao,
        'Colégio e Coelho Neto' AS bairros_texto,
        'junto às estações do metrô' AS local_terminais,
        'Colégio terá seis pontos cobertos, quadras e espaço pra criançada. Coelho Neto vai receber linhas municipais e intermunicipais, com melhorias no entorno da estação. As obras já iniciaram no Colégio e logo seguirão pra Coelho Neto.' AS detalhes
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
  cfg.bairros_texto AS bairros,
  cfg.local_terminais,
  cfg.detalhes
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
