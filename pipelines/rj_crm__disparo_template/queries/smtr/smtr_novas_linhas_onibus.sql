WITH
  -- 1. Configuração centralizada das campanhas (regras de público + textos do template)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        'ILHA' AS campanha,
        1 AS prioridade,
        ['FREGUESIA (ILHA)', 'COCOTA', 'GALEAO', 'CIDADE UNIVERSITARIA'] AS bairros,
        ['323', '327', '328', 'SP328', '634', '635', '696', '901', '915', '616', '913', 'TCMAR', 'TCFAM', 'TCTJ1', 'TCTJ2'] AS servicos,
        ['BRT_MARE', 'BRT_TERMINAL_FUNDAO', 'BRT_GALEAO'] AS paradas,
        'Novas linhas de ônibus ligam a Ilha do Governador ao metrô de Del Castilho' AS chamada,
        'Desde 7 de novembro, duas novas linhas do serviço Conexão BRT fazem a ligação direta entre a Ilha do Governador e a estação de metrô de Del Castilho' AS descricao,
        '🚌 *Linha 91:* Bananal ↔️ Del Castilho, passando pela Estação das Barcas de Cocotá.   |   🚌 *Linha 95:* Aeroporto do Galeão ↔️ Del Castilho, atendendo os terminais 1 e 2 do aeroporto' AS linhas,
        'as duas linhas passam pelo Terminal Fundão do BRT e pela estação de trem de Bonsucesso' AS itinerario,
        'das 4h às 23h' AS horario
      ),
      STRUCT(
        'JACAREPAGUA' AS campanha,
        2 AS prioridade,
        ['BARRA OLIMPICA', 'CAMORIM', 'CURICICA', 'TAQUARA', 'PECHINCHA', 'FREGUESIA (JACAREPAGUA)', 'ANIL'] AS bairros,
        ['611', '390', '380', '881', 'SN810', '861', '932', 'TLCEO', 'TLMOE', 'TLRIC', 'TLOLO', 'TLASA', 'TLOTS', 'TCTAQ'] AS servicos,
        ['BRT_CENTRO_OLIMPICO', 'BRT_MORRO_DO_OUTEIRO', 'BRT_RIOCENTRO', 'BRT_OLOF_PALME', 'BRT_ASA_BRANCA', 'BRT_OUTEIRO_SANTO', 'BRT_TAQUARA'] AS paradas,
        'Nova linha de ônibus liga o Terminal Centro Olímpico ao Anil' AS chamada,
        'A Prefeitura do Rio criou a Linha 54 do serviço Conexão BRT, que faz a ligação entre o Terminal Centro Olímpico e o bairro do Anil, ampliando as opções de deslocamento pela Barra e por Jacarepaguá' AS descricao,
        '🚌 *Linha 54:* Terminal Centro Olímpico ↔️ Anil' AS linhas,
        'a linha passa pelo Riocentro, Camorim, Curicica, Largo da Preguiça, Centro da Taquara, Largo do Pechincha, Praça Mac Gregor, Largo da Freguesia e Park Jacarepaguá' AS itinerario,
        'todos os dias, das 4h às 23h' AS horario
      )
    ])
  ),


  -- 2. Geometrias dos bairros-alvo a partir da base oficial do DataRio
  bairros_geo AS (
    SELECT
      cfg.campanha,
      cfg.prioridade,
      b.nome AS nome_bairro_oficial,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') AS bairro_norm,
      b.geometry
    FROM `datario.dados_mestres.bairro` AS b
    INNER JOIN config_campanhas AS cfg
      ON REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') IN UNNEST(cfg.bairros)
    WHERE
      b.geometry IS NOT NULL
  ),


  -- 2b. Pontos das portas por CEP (numeração de porta oficial do DataRio),
  --     usados para validar se o ponto da RMI está perto do CEP cadastrado.
  portas_cep AS (
    SELECT
      LPAD(REGEXP_REPLACE(np.cep, r'\D', ''), 8, '0') AS cep,
      np.geometry
    FROM `datario.dados_mestres.numero_porta` AS np
    WHERE np.cep IS NOT NULL
      AND np.geometry IS NOT NULL
  ),


  -- 3. Base elegível da RMI (aplica filtros cadastrais, normaliza texto e cria ponto geográfico)
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
      ) AS nome_sobrenome,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(endereco.principal.bairro)), NFD), r'\pM', '') AS bairro_norm,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(IFNULL(endereco.principal.municipio, 'RIO DE JANEIRO'))), NFD), r'\pM', '') AS municipio_norm,
      UPPER(TRIM(IFNULL(endereco.principal.estado, 'RJ'))) AS estado_norm,
      LPAD(REGEXP_REPLACE(IFNULL(endereco.principal.cep, ''), r'\D', ''), 8, '0') AS cep_norm,
      SAFE.ST_GEOGPOINT(
          SAFE_CAST(endereco.principal.longitude AS FLOAT64),
          SAFE_CAST(endereco.principal.latitude AS FLOAT64)
        ) AS ponto_geo
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
    WHERE
      obito.indicador = FALSE
      AND menor_idade = FALSE
      AND (nome IS NOT NULL OR nome_social IS NOT NULL)
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
  ),


  -- 3b. Ponto geográfico validado: ponto da RMI dentro do polígono de um bairro-alvo e
  --     coerente com o endereço cadastrado, ou seja:
  --       - o bairro onde o ponto cai é o bairro de endereco.principal.bairro
  --         (ignorando sufixo entre parênteses), OU
  --       - o ponto está a no máximo 500 m de alguma porta do CEP de endereco.principal.cep.
  --     Evita usar pontos georreferenciados errado.
  ponto_validado AS (
    SELECT DISTINCT
      rmi.cpf,
      rmi.cpf_particao,
      bg.campanha,
      bg.prioridade
    FROM base_rmi AS rmi
    INNER JOIN bairros_geo AS bg
      ON ST_INTERSECTS(bg.geometry, rmi.ponto_geo)
    LEFT JOIN portas_cep AS p
      ON p.cep = rmi.cep_norm
      AND ST_DWITHIN(p.geometry, rmi.ponto_geo, 500)
    WHERE
      rmi.ponto_geo IS NOT NULL
      AND (
        bg.bairro_norm = rmi.bairro_norm
        -- aceita grafia sem sufixo: 'FREGUESIA' bate com 'FREGUESIA (ILHA)' / 'FREGUESIA (JACAREPAGUA)'
        OR REGEXP_REPLACE(bg.bairro_norm, r'\s*\(.*\)', '') = rmi.bairro_norm
        OR p.cep IS NOT NULL
      )
  ),


  -- 4. Consolida todas as fontes de elegibilidade (bairro texto, geometria, linhas e paradas BRT)
  registros_classificados AS (
    -- 4a. Moradores por nome do bairro cadastrado
    SELECT
      rmi.cpf,
      rmi.cpf_particao,
      cfg.campanha,
      cfg.prioridade
    FROM base_rmi AS rmi
    INNER JOIN config_campanhas AS cfg
      ON rmi.bairro_norm IN UNNEST(cfg.bairros)
    WHERE
      rmi.municipio_norm = 'RIO DE JANEIRO'
      AND rmi.estado_norm = 'RJ'


    UNION ALL


    -- 4b. Moradores por interseção espacial, só com ponto validado por bairro ou CEP (ver 3b).
    --     Pontos incoerentes são descartados; o CPF ainda pode entrar pelo 4a (bairro cadastrado),
    --     4c (linhas) ou 4d (paradas).
    SELECT
      cpf,
      cpf_particao,
      campanha,
      prioridade
    FROM ponto_validado


    UNION ALL


    -- 4c. Usuários das linhas de ônibus
    SELECT
      srv.cpf,
      srv.cpf_particao,
      cfg.campanha,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.servicos)


    UNION ALL


    -- 4d. Usuários das estações BRT
    SELECT
      par.cpf,
      par.cpf_particao,
      cfg.campanha,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_paradas_20261124` AS par
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(par.parada)) IN UNNEST(cfg.paradas)
  ),


  -- 5. Deduplica por CPF respeitando a prioridade (1 = ILHA, 2 = JACAREPAGUA)
  publico_unico AS (
    SELECT
      cpf,
      cpf_particao,
      campanha
    FROM registros_classificados
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY cpf_particao, cpf
      ORDER BY prioridade ASC
    ) = 1
  ),


  -- 5b. Payload completo do público (antes dos filtros de disparo)
  payload AS (
    SELECT
      rmi.telefone,
      pub.cpf,
      rmi.nome_sobrenome,
      cfg.chamada,
      cfg.descricao,
      cfg.linhas,
      cfg.itinerario,
      cfg.horario
    FROM publico_unico AS pub
    INNER JOIN base_rmi AS rmi
      ON rmi.cpf_particao = pub.cpf_particao
    INNER JOIN config_campanhas AS cfg
      ON cfg.campanha = pub.campanha
    WHERE
      rmi.telefone IS NOT NULL
  ),


  -- 5c. Disparos de HSM de hoje (qualquer HSM, qualquer status) registrados no chatbot
  disparos_hoje AS (
    SELECT
      contato.cpf AS cpf,
      contato.contato_telefone AS telefone,
      hsm.nome_hsm AS nome_hsm,
      indicador_enviada,
      indicador_falha,
      falha_codigo
    FROM `rj-crm-registry.rmi_conversas.chatbot`
    WHERE
      data_particao >= CURRENT_DATE('America/Sao_Paulo')
      AND fonte = 'HSM'
  ),


  -- 5d. Quem já foi impactado hoje por ESTA HSM e não deve receber de novo:
  --     envio com sucesso (sent/delivered/read) ou falha 131026 (número sem WhatsApp)
  impactados_hsm_hoje AS (
    SELECT cpf, telefone
    FROM disparos_hoje
    WHERE
      nome_hsm = 'smtr_novas_linhas_onibus_v3'
      AND (
        indicador_enviada
        OR falha_codigo = '131026'
      )
  ),


  -- 5e. Quem teve falha hoje nesta HSM por outro motivo que não o 131026 e deve ser reenviado
  --     (ex.: 132018 'Param text cannot have new-line/tab characters...', causada pelo \n na variável linhas)
  falhas_reenviar AS (
    SELECT DISTINCT cpf
    FROM disparos_hoje
    WHERE
      nome_hsm = 'smtr_novas_linhas_onibus_v3'
      AND indicador_falha
      AND IFNULL(falha_codigo, '') != '131026'
      AND cpf IS NOT NULL
  )


-- 6. Payload final:
--    6a. público que não teve nenhum disparo hoje (qualquer HSM), por CPF e por telefone
SELECT p.*
FROM payload AS p
WHERE
  p.cpf NOT IN (SELECT cpf FROM disparos_hoje WHERE cpf IS NOT NULL)
  AND p.telefone NOT IN (SELECT telefone FROM disparos_hoje WHERE telefone IS NOT NULL)

UNION DISTINCT

--    6b. + quem falhou hoje nesta HSM com falha diferente de 131026, desde que não tenha sido
--        impactado hoje por ela de outra forma (sucesso ou 131026), por CPF e por telefone
SELECT p.*
FROM payload AS p
WHERE
  p.cpf IN (SELECT cpf FROM falhas_reenviar)
  AND p.cpf NOT IN (SELECT cpf FROM impactados_hsm_hoje WHERE cpf IS NOT NULL)
  AND p.telefone NOT IN (SELECT telefone FROM impactados_hsm_hoje WHERE telefone IS NOT NULL)
