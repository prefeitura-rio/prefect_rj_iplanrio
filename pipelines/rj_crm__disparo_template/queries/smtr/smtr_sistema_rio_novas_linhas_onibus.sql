WITH
  -- 1. Configuração centralizada das frentes do Sistema RIO (regras de público + textos do template)
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['SANTA CRUZ'] AS bairros,
        ['804', '807', '809', '825', '840', '842', '849', '868', '870', '892', '898'] AS servicos,
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
        ['CAMPO GRANDE'] AS bairros,
        ['808', '821', '822', '833', '841', '869', '893'] AS servicos,
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


  -- 2. Bairros oficiais do DataRio (nome normalizado + geometria) vinculados à configuração
  bairros_alvo AS (
    SELECT
      cfg.prioridade,
      REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') AS bairro_norm,
      b.geometry
    FROM `datario.dados_mestres.bairro` AS b
    INNER JOIN config_campanhas AS cfg
      ON REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(b.nome)), NFD), r'\pM', '') IN UNNEST(cfg.bairros)
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
      `rj-crm-registry.udf.FORMAT_NAME`(
        COALESCE(nome_social, nome), TRUE
      ) AS nome_sobrenome,
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
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR','EVITAR')
  ),


  -- 4. Consolida elegibilidade (moradores por nome/geometria + passageiros das linhas SPPO)
  registros_classificados AS (
    -- 4a. Moradores por nome do bairro cadastrado
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


    -- 4b. Moradores por interseção espacial (lat/long dentro do polígono do bairro)
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


    -- 4c. Passageiros das linhas de SPPO (atualizar nome da tabela de origem na sandbox)
    SELECT
      srv.cpf_particao,
      cfg.prioridade
    FROM `rj-smtr-sandbox.disparo_mensagem.comunicado_conexao_brt_ilha_servicos_20261124` AS srv
    INNER JOIN config_campanhas AS cfg
      ON UPPER(TRIM(srv.servico)) IN UNNEST(cfg.servicos)
  ),


  -- 5. Deduplica por CPF (cpf_particao) escolhendo apenas 1 frente de envio
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
  rmi.nome_sobrenome,
  cfg.descricao_onibus,
  cfg.bairro,
  cfg.gancho,
  cfg.recebimento,
  cfg.linhas,
  cfg.destaque,
  cfg.prazo
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade
