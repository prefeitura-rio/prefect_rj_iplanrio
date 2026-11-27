WITH
  -- 1. Configuração centralizada das 3 frentes de terminais existentes (Template 6)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['TBMGD'] AS servicos,
        ['TERMINAL_MARGARIDAS'] AS paradas,
        'Tá pensando em viajar? 🧳' AS chamada,
        'No Terminal Margaridas, em Irajá' AS local,
        'você pode embarcar pra São Paulo, Angra dos Reis e Resende, com saída todo dia' AS descricao,
        '📍 São Paulo — 22h\nDireto pro Terminal Tietê, sem parada em Campo Grande.\n📍 Angra dos Reis — 5h40 e 19h40\nPassando por Mangaratiba. Partiu Costa Verde? 🌊\n📍 Resende — 6h40\nPassando por Piraí, Volta Redonda e Barra Mansa.\n\n💡 Todas as rotas também têm volta pro Margaridas!' AS conexoes,
        'Pra chegar ao terminal, tem BRT 70 e 73, saindo do Gentileza, e os ônibus municipais 944, 685, 950, 951, 959 e SV774. Também tem conexão com Mesquita, São João de Meriti, Nova Iguaçu, Nilópolis, Belford Roxo e Queimados pelas linhas intermunicipais.' AS destaque
      ),
      STRUCT(
        2 AS prioridade,
        ['TBTIG'] AS servicos,
        ['TERMINAL_GENTILEZA'] AS paradas,
        'Quer chegar a diferentes regiões do Rio? 🏙️' AS chamada,
        'No Terminal Gentileza' AS local,
        'você encontra BRT, VLT e ônibus municipais reunidos num só lugar' AS descricao,
        '🚌 BRT Transbrasil — linhas 60 (Deodoro), 61 (Deodoro, expresso), 70 (Margaridas), 80 (Penha) e 90 (Fundão), além do serviço executivo direto pro Aeroporto do Galeão.\n🚊 VLT Carioca — Linha 1, até o Aeroporto Santos Dumont, e Linha 4, até a Praça XV, com acesso às barcas.\n🚌 Ônibus municipais — linhas pra São Conrado, Ipanema, Leblon, Copacabana, Urca, Cosme Velho e Barra da Tijuca.' AS conexoes,
        'O terminal fica a cerca de 500 metros da Rodoviária Novo Rio, facilitando a conexão com viagens rodoviárias.' AS destaque
      ),
      STRUCT(
        3 AS prioridade,
        ['TLDEO'] AS servicos,
        ['TERMINAL_DEODORO'] AS paradas,
        'Precisa se deslocar entre a Zona Norte, a Zona Oeste e o resto da cidade? 🚆' AS chamada,
        'No Terminal Deodoro' AS local,
        'você combina trem, BRT e ônibus municipais e intermunicipais num só lugar' AS descricao,
        '🚍 BRT Transolímpica — Linhas 51 (Deodoro x Recreio) e 52 (Deodoro x Alvorada), conectando a Zona Oeste à Barra da Tijuca e ao Recreio.\n🚍 BRT Transbrasil — linhas 60 (Gentileza, parador) e 61 (Gentileza, expresso), fazendo a ligação com a região central.\n🚌 Conexão BRT — linhas 67 (Campo Grande) e 68 (Bangu) Os dois serviços funcionam 24 horas.\n🚆 Trens metropolitanos — A estação integra os ramais Santa Cruz e Japeri, conectando a região diversos municípios da baixada fluminense.' AS conexoes,
        '🚌 Ônibus municipais e intermunicipais — O terminal conta com linhas alimentadoras que conectam a destinos da Zona Oeste (Anchieta, Realengo, Bangu, Campo Grande), Zona Norte (Bonsucesso, Madureira e Inhaúma, Méier) e Baixada Fluminense (Nilópolis, Mesquita e Belford Roxo)' AS destaque
      )
    ])
  ),
  -- 2. Base elegível da RMI
  base_rmi AS (
    SELECT
      cpf,
      cpf_particao,
      `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
        CONCAT(
          IFNULL(telefone.principal.ddi, '55'),
          IFNULL(telefone.principal.ddd, '21'),
          telefone.principal.valor
        )
      ) AS telefone,
      `rj-crm-registry.udf.FORMAT_NAME`(
        COALESCE(nome_social, nome), TRUE
      ) AS nome_sobrenome
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
    WHERE
      obito.indicador = FALSE
      AND menor_idade = FALSE
      AND (nome IS NOT NULL OR nome_social IS NOT NULL)
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
  ),
  -- 3. Consolida passageiros que embarcam nos terminais (via código de serviço e parada)
  registros_classificados AS (
    -- 3a. Embarques nos terminais mapeados na coluna servico (TBMGD, TBTIG, TLDEO)
    SELECT
      srv.cpf_particao,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.servicos)
    UNION ALL
    -- 3b. Embarques identificados na tabela de paradas (raio/estação)
    SELECT
      par.cpf_particao,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_paradas_20261124` AS par
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(par.parada)) IN UNNEST(cfg.paradas)
  ),
  -- 4. Deduplica por CPF (cpf_particao) garantindo 1 disparo por pessoa
  publico_unico AS (
    SELECT
      cpf_particao,
      prioridade
    FROM registros_classificados
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY cpf_particao
      ORDER BY prioridade ASC
    ) = 1
  )
-- 5. Monta o payload final para disparo
SELECT
  rmi.telefone,
  rmi.cpf,
  rmi.nome_sobrenome,
  cfg.chamada,
  cfg.local,
  cfg.descricao,
  cfg.conexoes,
  cfg.destaque
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade
