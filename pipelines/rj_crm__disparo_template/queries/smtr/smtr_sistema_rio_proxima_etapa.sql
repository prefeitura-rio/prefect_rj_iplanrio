WITH
  -- 1. Configuração da campanha (regiões administrativas, serviços SPPO e variáveis do template)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['CAMPO GRANDE', 'SANTA CRUZ', 'GUARATIBA', 'BANGU', 'REALENGO'] AS regioes_administrativas,
        CAST([] AS ARRAY<STRING>) AS servicos,
        'Zona Oeste' AS regiao,
        'Guaratiba, Santa Cruz, Campo Grande e Bangu, Realengo' AS bairros,
        'primeiro trimestre de 2027' AS previsao,
        '654' AS frota_atual,
        '1.006' AS frota_nova
      )
    ])
  ),
  -- 2. Bairros das Regiões Administrativas listadas no DataRio
  bairros_alvo AS (
    SELECT
      cfg.prioridade,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') AS bairro_norm,
      b.geometry
    FROM `datario.dados_mestres.bairro` AS b
    INNER JOIN config_campanhas AS cfg
      ON REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome_regiao_administrativa)), NFD), r'\pM', '')
        IN UNNEST(cfg.regioes_administrativas)
  ),
  -- 3. Base elegível da RMI com endereço normalizado e ponto geográfico (lat/long)
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
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
  ),
  -- 4. Consolida elegibilidade (moradores da região por nome/geometria + passageiros SPPO)
  registros_classificados AS (
    -- 4a. Moradores por nome do bairro cadastrado
    SELECT
      rmi.cpf_particao,
      ba.prioridade
    FROM base_rmi AS rmi
    INNER JOIN bairros_alvo AS ba
      ON rmi.bairro_norm = ba.bairro_norm
    WHERE
      rmi.municipio_norm = 'RIO DE JANEIRO'
      AND rmi.estado_norm = 'RJ'
    UNION ALL
    -- 4b. Moradores por interseção espacial (lat/long dentro do polígono dos bairros)
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
    -- 4c. Passageiros das linhas de SPPO (atualizar nome da tabela na sandbox)
    SELECT
      srv.cpf_particao,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.servicos)
  ),
  -- 5. Deduplica por CPF (cpf_particao)
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
-- 6. Monta o payload final para disparo
SELECT
  rmi.telefone,
  rmi.cpf,
  cfg.regiao,
  cfg.bairros,
  cfg.previsao,
  cfg.frota_atual,
  cfg.frota_nova
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade
