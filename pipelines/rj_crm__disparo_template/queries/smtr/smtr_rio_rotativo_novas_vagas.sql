WITH
  -- 1. Configuração centralizada das frentes do Rio Rotativo Digital
  config_campanhas AS (
    SELECT * FROM UNNEST([
      STRUCT(
        1 AS prioridade,
        ['IMPERIAL DE SAO CRISTOVAO', 'SAO CRISTOVAO'] AS bairros,
        'frequenta a Quinta da Boa Vista' AS relacao_local,
        '113' AS numero_vagas,
        'nas ruas Dom Meinrado e General Herculano Gomes e na Avenida do Exército' AS logradouros
      ),
      STRUCT(
        2 AS prioridade,
        ['HUMAITA', 'BOTAFOGO'] AS bairros,
        'estaciona no Humaitá' AS relacao_local,
        '331' AS numero_vagas,
        'nas ruas Humaitá, Marques, Conde de Irajá, Martins Ferreira e Capistrano de Abreu' AS logradouros
      ),
      STRUCT(
        3 AS prioridade,
        ['MARACANA'] AS bairros,
        'estaciona no entorno do Maracanã' AS relacao_local,
        '240' AS numero_vagas,
        'nas ruas General Canabarro, Ibituruna, Morais e Silva, Pedro Guedes e Visconde de Cairu' AS logradouros
      ),
      STRUCT(
        4 AS prioridade,
        ['MADUREIRA', 'OSVALDO CRUZ', 'OSWALDO CRUZ'] AS bairros,
        'estaciona em Madureira' AS relacao_local,
        '245' AS numero_vagas,
        'nas ruas Antônio de Abreu, Clara Nunes, Maria de Freitas, Dagmar da Fonseca, Américo Brasiliense, Manuel Marques, Alcina, São Geraldo e Dona Clara' AS logradouros
      ),
      STRUCT(
        5 AS prioridade,
        ['RECREIO DOS BANDEIRANTES'] AS bairros,
        'frequenta a Praia da Reserva' AS relacao_local,
        '1.800' AS numero_vagas,
        'ao longo da Avenida Lúcio Costa, no trecho da Praia da Reserva' AS logradouros
      ),
      STRUCT(
        6 AS prioridade,
        ['CAMPO GRANDE'] AS bairros,
        'circula por Campo Grande' AS relacao_local,
        '250' AS numero_vagas,
        'nas ruas Augusto Vasconcelos, Amaral Costa, Agostinho Coelho, Engenheiro Trindade, José Ferreira, Guaraí, Uruna, Tatuoca, Viúva Dantas, Doutor Caetano de Faria Castro, Mário Barbosa, Camanducaia e Sacramento Blake' AS logradouros
      ),
      STRUCT(
        7 AS prioridade,
        ['LEME', 'COPACABANA', 'IPANEMA', 'LEBLON'] AS bairros,
        'frequenta as praias do Leme, Copacabana, Ipanema e Leblon' AS relacao_local,
        'mais de 1.700' AS numero_vagas,
        'distribuídas pelos quatro bairros' AS logradouros
      ),
      STRUCT(
        8 AS prioridade,
        ['BARRA DA TIJUCA', 'SAO CONRADO'] AS bairros,
        'frequenta a Barra da Tijuca e São Conrado' AS relacao_local,
        '1.400' AS numero_vagas,
        'na Barra da Tijuca, no trecho entre a Praia dos Amores e o Alvorada, além de vagas em São Conrado' AS logradouros
      ),
      STRUCT(
        9 AS prioridade,
        ['TIJUCA'] AS bairros,
        'circula pela Tijuca' AS relacao_local,
        '187' AS numero_vagas,
        'nas ruas Campos Sales, Afonso Pena, Martins Pena e Marechal Marques Porto' AS logradouros
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
      `rj-crm-registry.udf.FORMAT_NAME`(
        COALESCE(nome_social, nome), TRUE
      ) AS nome_sobrenome,
      endereco.principal.bairro AS bairro_original,
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
      AND telefone.principal.estrategia_envio IN ('ENVIAR', 'TESTAR')
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
        -- aceita grafia sem sufixo: 'FREGUESIA' bate com 'FREGUESIA (ILHA)' / 'FREGUESIA (JACAREPAGUA)'
        OR REGEXP_REPLACE(ba.bairro_norm, r'\s*\(.*\)', '') = rmi.bairro_norm
        OR p.cep IS NOT NULL
      )
  ),


  -- 4. Consolida elegibilidade por nome do bairro OU interseção espacial (geometria)
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


    -- 4b. Moradores por interseção espacial, só com ponto validado por bairro ou CEP (ver 3b).
    --     Pontos incoerentes são descartados; o CPF ainda pode entrar pelo 4a (bairro cadastrado).
    SELECT
      cpf_particao,
      prioridade
    FROM ponto_validado
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
  rmi.bairro_original AS bairro,
  cfg.relacao_local,
  cfg.numero_vagas,
  cfg.logradouros
FROM publico_unico AS pub
INNER JOIN base_rmi AS rmi
  ON rmi.cpf_particao = pub.cpf_particao
INNER JOIN config_campanhas AS cfg
  ON cfg.prioridade = pub.prioridade;
