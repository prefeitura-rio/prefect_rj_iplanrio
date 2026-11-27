WITH
  -- 1. Configuração centralizada das 4 frentes de novos terminais (Template 4)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['TOCGD'] AS servicos,
        ['TERMINAL_CAMPO_GRANDE'] AS paradas,
        'Campo Grande' AS bairro,
        'um novo Terminal Intermodal, entre a Estrada da Caroba e a Rua Campo Grande' AS terminal,
        'Serão 33 linhas municipais e intermunicipais, com conexão ao BRT, plataformas cobertas, acessibilidade, banheiros e bicicletário.' AS detalhes
      ),
      STRUCT(
        2 AS prioridade,
        ['TOSTC'] AS servicos,
        ['TERMINAL_SANTA_CRUZ'] AS paradas,
        'Santa Cruz' AS bairro,
        'o Terminal Bairro Imperial, reunindo ônibus, BRT e vans no centro do bairro, perto das ruas Álvaro Alberto, Felipe Cardoso e Dom Pedro I' AS terminal,
        'Vai ter plataformas organizadas, acessibilidade, banheiros, lojas e bicicletário para até 300 vagas. 📅 A previsão é começar a funcionar no segundo semestre de 2027. Mais facilidade pra fazer sua conexão e seguir viagem! 💙' AS detalhes
      ),
      STRUCT(
        3 AS prioridade,
        ['M-CLG'] AS servicos,
        ['TERMINAL_COLEGIO'] AS paradas,
        'Colégio' AS bairro,
        'um terminal junto à estação de metrô, com seis pontos de ônibus cobertos e espaço organizado pra embarcar e desembarcar' AS terminal,
        'Vai ter cobertura e melhorias no entorno, com duas quadras e área de lazer pra criançada.' AS detalhes
      ),
      STRUCT(
        4 AS prioridade,
        ['M-CNT'] AS servicos,
        ['TERMINAL_COELHO_NETO'] AS paradas,
        'Coelho Neto' AS bairro,
        'um terminal junto à estação de metrô, reunindo ônibus municipais e intermunicipais em um espaço organizado pra embarcar e desembarcar' AS terminal,
        'A novidade vai facilitar a viagem de passageiros da zona norte, zona oeste, Itaguaí, Seropédica e Cabuçu.' AS detalhes
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
      ) AS telefone
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
    WHERE
      obito.indicador = FALSE
      AND menor_idade = FALSE
      AND (nome IS NOT NULL OR nome_social IS NOT NULL)
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR', 'EVITAR')
  ),
  -- 3. Consolida passageiros por código de estação/terminal (serviço) e por parada
  registros_classificados AS (
    -- 3a. Embarques nas estações BRT / Metrô mapeados na coluna servico
    SELECT
      srv.cpf_particao,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.servicos)
    UNION ALL
    -- 3b. Embarques identificados na tabela de paradas
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
  cfg.bairro,
  cfg.terminal,
  cfg.detalhes
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade;
