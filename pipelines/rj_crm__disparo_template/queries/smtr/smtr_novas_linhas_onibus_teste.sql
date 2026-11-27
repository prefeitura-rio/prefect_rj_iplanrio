-- TESTE - SMTR - Novas linhas de ônibus Conexão BRT (Ilha e Jacarepaguá)
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_novas_linhas_onibus.sql), enviados só para os contatos de teste.
-- Cada contato recebe uma campanha (prioridade): troque o número em contatos_teste para
-- ver a outra versão. O flow filtra CPF duplicado, então um contato recebe uma campanha por vez.
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        'ILHA' AS campanha,
        1 AS prioridade,
        'Novas linhas de ônibus ligam a Ilha do Governador ao metrô de Del Castilho' AS chamada,
        'Desde 7 de novembro, duas novas linhas do serviço Conexão BRT fazem a ligação direta entre a Ilha do Governador e a estação de metrô de Del Castilho' AS descricao,
        '🚌 Linha 91: Bananal ↔️ Del Castilho, passando pela Estação das Barcas de Cocotá.\n🚌 Linha 95: Aeroporto do Galeão ↔️ Del Castilho, atendendo os terminais 1 e 2 do aeroporto' AS linhas,
        'as duas linhas passam pelo Terminal Fundão do BRT e pela estação de trem de Bonsucesso' AS itinerario,
        'das 4h às 23h' AS horario
      ),
      STRUCT(
        'JACAREPAGUA' AS campanha,
        2 AS prioridade,
        'Nova linha de ônibus liga o Terminal Centro Olímpico ao Anil' AS chamada,
        'A Prefeitura do Rio criou a Linha 54 do serviço Conexão BRT, que faz a ligação entre o Terminal Centro Olímpico e o bairro do Anil, ampliando as opções de deslocamento pela Barra e por Jacarepaguá' AS descricao,
        'Linha 54: Terminal Centro Olímpico ↔️ Anil' AS linhas,
        'a linha passa pelo Riocentro, Camorim, Curicica, Largo da Preguiça, Centro da Taquara, Largo do Pechincha, Praça Mac Gregor, Largo da Freguesia e Park Jacarepaguá' AS itinerario,
        'todos os dias, das 4h às 23h' AS horario
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 'patricia' AS nome, 1 AS prioridade),
      -- STRUCT('5521980375732' AS telefone, '00000000349' AS cpf, 'rodolpho' AS nome, 2 AS prioridade),
      STRUCT('5521989190512' AS telefone, '00000000032' AS cpf, 'maria salesforce' AS nome, 2 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  `rj-crm-registry.udf.FORMAT_NAME`(t.nome, TRUE) AS nome_sobrenome,
  cfg.chamada,
  cfg.descricao,
  cfg.linhas,
  cfg.itinerario,
  cfg.horario
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
