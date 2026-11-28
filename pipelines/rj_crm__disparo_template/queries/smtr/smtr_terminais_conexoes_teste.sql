-- TESTE - SMTR - Terminais: conexões disponíveis (Template 6)
-- Mesmos textos de config_campanhas da query de produção
-- (smtr_terminais_conexoes.sql), enviados só para os contatos de teste.
-- Cada contato recebe uma frente (prioridade): troque o número em contatos_teste para
-- ver as outras versões (1 Margaridas, 2 Gentileza, 3 Deodoro).
WITH
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        'Tá pensando em viajar? 🧳' AS chamada,
        'No Terminal Margaridas, em Irajá' AS local,
        'você pode embarcar pra São Paulo, Angra dos Reis e Resende, com saída todo dia' AS descricao,
        '📍 São Paulo — 22h\nDireto pro Terminal Tietê, sem parada em Campo Grande.\n📍 Angra dos Reis — 5h40 e 19h40\nPassando por Mangaratiba. Partiu Costa Verde? 🌊\n📍 Resende — 6h40\nPassando por Piraí, Volta Redonda e Barra Mansa.\n\n💡 Todas as rotas também têm volta pro Margaridas!' AS conexoes,
        'Pra chegar ao terminal, tem BRT 70 e 73, saindo do Gentileza, e os ônibus municipais 944, 685, 950, 951, 959 e SV774. Também tem conexão com Mesquita, São João de Meriti, Nova Iguaçu, Nilópolis, Belford Roxo e Queimados pelas linhas intermunicipais.' AS destaque
      ),
      STRUCT(
        2 AS prioridade,
        'Quer chegar a diferentes regiões do Rio? 🏙️' AS chamada,
        'No Terminal Gentileza' AS local,
        'você encontra BRT, VLT e ônibus municipais reunidos num só lugar' AS descricao,
        '🚌 BRT Transbrasil — linhas 60 (Deodoro), 61 (Deodoro, expresso), 70 (Margaridas), 80 (Penha) e 90 (Fundão), além do serviço executivo direto pro Aeroporto do Galeão.\n🚊 VLT Carioca — Linha 1, até o Aeroporto Santos Dumont, e Linha 4, até a Praça XV, com acesso às barcas.\n🚌 Ônibus municipais — linhas pra São Conrado, Ipanema, Leblon, Copacabana, Urca, Cosme Velho e Barra da Tijuca.' AS conexoes,
        'O terminal fica a cerca de 500 metros da Rodoviária Novo Rio, facilitando a conexão com viagens rodoviárias.' AS destaque
      ),
      STRUCT(
        3 AS prioridade,
        'Precisa se deslocar entre a Zona Norte, a Zona Oeste e o resto da cidade? 🚆' AS chamada,
        'No Terminal Deodoro' AS local,
        'você combina trem, BRT e ônibus municipais e intermunicipais num só lugar' AS descricao,
        '🚍 BRT Transolímpica — Linhas 51 (Deodoro x Recreio) e 52 (Deodoro x Alvorada), conectando a Zona Oeste à Barra da Tijuca e ao Recreio.\n🚍 BRT Transbrasil — linhas 60 (Gentileza, parador) e 61 (Gentileza, expresso), fazendo a ligação com a região central.\n🚌 Conexão BRT — linhas 67 (Campo Grande) e 68 (Bangu) Os dois serviços funcionam 24 horas.\n🚆 Trens metropolitanos — A estação integra os ramais Santa Cruz e Japeri, conectando a região diversos municípios da baixada fluminense.' AS conexoes,
        '🚌 Ônibus municipais e intermunicipais — O terminal conta com linhas alimentadoras que conectam a destinos da Zona Oeste (Anchieta, Realengo, Bangu, Campo Grande), Zona Norte (Bonsucesso, Madureira e Inhaúma, Méier) e Baixada Fluminense (Nilópolis, Mesquita e Belford Roxo)' AS destaque
      )
    ])
  ),

  contatos_teste AS (
    SELECT * FROM UNNEST([
      STRUCT('5511984677798' AS telefone, '00000000350' AS cpf, 'patricia' AS nome, 1 AS prioridade),
      STRUCT('5521980375732' AS telefone, '00000000349' AS cpf, 'rodolpho' AS nome, 2 AS prioridade),
      STRUCT('5521989190512' AS telefone, '00000000032' AS cpf, 'maria salesforce' AS nome, 3 AS prioridade)
    ])
  )

SELECT
  t.telefone,
  t.cpf,
  `rj-crm-registry.udf.FORMAT_NAME`(t.nome, TRUE) AS nome_sobrenome,
  cfg.chamada,
  cfg.local,
  cfg.descricao,
  cfg.conexoes,
  cfg.destaque
FROM contatos_teste AS t
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = t.prioridade
