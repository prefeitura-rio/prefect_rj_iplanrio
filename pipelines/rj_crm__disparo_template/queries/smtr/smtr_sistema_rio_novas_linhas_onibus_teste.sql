-- TESTE - SMTR - Sistema Rio - Novas linhas de ônibus
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_sistema_rio_novas_linhas_onibus.sql), enviados só para os contatos de teste.
-- Cada contato recebe uma frente (prioridade): troque o número em contatos_teste para
-- ver a outra versão. O flow filtra CPF duplicado, então um contato recebe uma frente por vez.
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        'ônibus amarelos com faixa roxa' AS descricao_onibus,
        'Santa Cruz' AS bairro,
        'Tipo guaraná de açaí.' AS gancho,
        'foi o primeiro a receber: 115 veículos novos do sistema, que se juntaram aos 350 ônibus zero quilômetro que já circulavam na Zona Oeste' AS recebimento,
        '804, 807, 809, 825, 840, 842, 849, 868, 870, 892 e 898' AS linhas,
        'E os modelos de piso baixo também facilitam o embarque! 😉' AS destaque,
        'dezembro' AS prazo
      ),
      STRUCT(
        2 AS prioridade,
        'novos ônibus laranja' AS descricao_onibus,
        'Campo Grande' AS bairro,
        '🍊' AS gancho,
        'recebeu 54 ônibus zerados' AS recebimento,
        '808, 821, 822, 833, 841, 869 e 893' AS linhas,
        'Big Field também ganhou a primeira garagem pública do Sistema RIO, pra apoiar a operação e a manutenção dos veículos.' AS destaque,
        'dezembro' AS prazo
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 'patricia' AS nome, 1 AS prioridade),
      STRUCT('5521980375732' AS telefone, '00000000349' AS cpf, 'rodolpho' AS nome, 2 AS prioridade),
      STRUCT('5521989190512' AS telefone, '00000000032' AS cpf, 'maria salesforce' AS nome, 1 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  `rj-crm-registry.udf.FORMAT_NAME`(t.nome, TRUE) AS nome_sobrenome,
  cfg.descricao_onibus,
  cfg.bairro,
  cfg.gancho,
  cfg.recebimento,
  cfg.linhas,
  cfg.destaque,
  cfg.prazo
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
