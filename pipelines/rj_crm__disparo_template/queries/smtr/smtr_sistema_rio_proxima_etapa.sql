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
      rmi.cpf_particao,
      ba.prioridade
    FROM base_rmi AS rmi
    INNER JOIN bairros_alvo AS ba
      ON ST_INTERSECTS(ba.geometry, rmi.ponto_geo)
    LEFT JOIN portas_cep AS p
      ON p.cep = rmi.cep_norm
      AND ST_DWITHIN(p.geometry, rmi.ponto_geo, 500)
    WHERE
      rmi.ponto_geo IS NOT NULL
      AND ba.geometry IS NOT NULL
      AND (
        ba.bairro_norm = rmi.bairro_norm
        OR REGEXP_REPLACE(ba.bairro_norm, r'\s*\(.*\)', '') = rmi.bairro_norm
        OR p.cep IS NOT NULL
      )
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
    -- 4b. Moradores por interseção espacial, só com ponto validado por bairro ou CEP (ver 3b).
    --     Pontos incoerentes são descartados; o CPF ainda pode entrar pelo 4a (bairro cadastrado)
    --     ou 4c (linhas).
    SELECT
      cpf_particao,
      prioridade
    FROM ponto_validado
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
