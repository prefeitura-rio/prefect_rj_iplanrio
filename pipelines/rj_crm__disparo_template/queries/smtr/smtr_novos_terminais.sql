WITH
  -- 1. Configuração centralizada da campanha (público-alvo, público não-alvo e textos do Template 5)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['COLEGIO', 'COELHO NETO'] AS bairros,
        ['3077O00014C0', '3110O00061C2'] AS stop_ids_referencia,
        1000.0 AS raio_metros,
        STRUCT(
          ['M-CLG', 'M-CNT'] AS servicos,
          ['TERMINAL_COLEGIO', 'TERMINAL_COELHO_NETO'] AS paradas
        ) AS publico_nao_alvo,
        'Zona Norte' AS regiao,
        'Colégio e Coelho Neto' AS bairros_texto,
        'junto às estações do metrô' AS local_terminais,
        'Colégio terá seis pontos cobertos, quadras e espaço pra criançada. Coelho Neto vai receber linhas municipais e intermunicipais, com melhorias no entorno da estação. As obras já iniciaram no Colégio e logo seguirão pra Coelho Neto.' AS detalhes
      )
    ])
  ),
  -- 2. Geometrias oficiais dos bairros-alvo no DataRio
  bairros_alvo AS (
    SELECT
      cfg.prioridade,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') AS bairro_norm,
      b.geometry
    FROM `datario.dados_mestres.bairro` AS b
    INNER JOIN config_campanhas AS cfg
      ON REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') IN UNNEST(cfg.bairros)
  ),
  -- 3. Pontos de referência oficiais do GTFS (Metrô Colégio e Metrô Coelho Neto)
  stops_gtfs AS (
    SELECT
      cfg.prioridade,
      cfg.raio_metros,
      s.stop_id,
      ST_GEOGPOINT(s.stop_lon, s.stop_lat) AS ponto_estacao
    FROM `rj-smtr.gtfs.stops` AS s
    INNER JOIN config_campanhas AS cfg
      ON s.stop_id IN UNNEST(cfg.stop_ids_referencia)
    WHERE
      s.feed_start_date <= CURRENT_DATE('America/Sao_Paulo')
      AND s.stop_lat IS NOT NULL
      AND s.stop_lon IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY s.stop_id
      ORDER BY s.feed_start_date DESC
    ) = 1
  ),
  -- 4. Base elegível da RMI com endereço normalizado e ponto geográfico (lat/long)
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
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(endereco.principal.bairro)), NFD), r'\pM', '') AS bairro_norm,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(IFNULL(endereco.principal.municipio, 'RIO DE JANEIRO'))), NFD), r'\pM', '') AS municipio_norm,
      UPPER(TRIM(IFNULL(endereco.principal.estado, 'RJ'))) AS estado_norm,
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
  -- 5. Público não-alvo (já impactado no Template 4) lido a partir da config_campanhas
  publico_nao_alvo AS (
    SELECT DISTINCT
      cfg.prioridade,
      srv.cpf_particao
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.publico_nao_alvo.servicos)
    UNION DISTINCT
    SELECT DISTINCT
      cfg.prioridade,
      par.cpf_particao
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_paradas_20261124` AS par
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(par.parada)) IN UNNEST(cfg.publico_nao_alvo.paradas)
  ),
  -- 6. Consolida elegibilidade (moradores dos bairros + moradores no raio de 1 km dos pontos GTFS)
  registros_classificados AS (
    -- 6a. Moradores por nome do bairro cadastrado
    SELECT
      rmi.cpf_particao,
      cfg.prioridade
    FROM base_rmi AS rmi
    INNER JOIN config_campanhas AS cfg
      ON rmi.bairro_norm IN UNNEST(cfg.bairros)
    WHERE
      rmi.municipio_norm = 'RIO DE JANEIRO'
      AND rmi.estado_norm = 'RJ'
    UNION ALL
    -- 6b. Moradores por interseção espacial (lat/long dentro do polígono dos bairros)
    SELECT
      rmi.cpf_particao,
      ba.prioridade
    FROM base_rmi AS rmi
    INNER JOIN bairros_alvo AS ba
      ON ST_INTERSECTS(ba.geometry, rmi.ponto_geo)
    WHERE
      rmi.ponto_geo IS NOT NULL
      AND ba.geometry IS NOT NULL
    UNION ALL
    -- 6c. Moradores em um raio de até 1 km dos pontos GTFS (Metrô Colégio e Metrô Coelho Neto)
    SELECT
      rmi.cpf_particao,
      st.prioridade
    FROM base_rmi AS rmi
    INNER JOIN stops_gtfs AS st
      ON ST_DWITHIN(st.ponto_estacao, rmi.ponto_geo, st.raio_metros)
    WHERE
      rmi.ponto_geo IS NOT NULL
  ),
  -- 7. Deduplica por CPF (cpf_particao) e exclui o público não-alvo da respectiva campanha
  publico_unico AS (
    SELECT DISTINCT
      reg.cpf_particao,
      reg.prioridade
    FROM registros_classificados AS reg
    LEFT JOIN publico_nao_alvo AS exc
      ON reg.cpf_particao = exc.cpf_particao
      AND reg.prioridade = exc.prioridade
    WHERE
      exc.cpf_particao IS NULL
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY reg.cpf_particao
      ORDER BY reg.prioridade ASC
    ) = 1
  )
-- 8. Monta o payload final para disparo (sem nome_sobrenome, conforme Template 5)
SELECT
  rmi.telefone,
  rmi.cpf,
  cfg.regiao,
  cfg.bairros_texto AS bairros,
  cfg.local_terminais,
  cfg.detalhes
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade
