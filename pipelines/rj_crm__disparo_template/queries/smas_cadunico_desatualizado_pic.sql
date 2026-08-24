-- ============================================================
-- Base CSV - Protocolo CadÚnico Atualizado
--
-- Público:
--   crianças ativas no PIC com protocolo irregular
--
-- Unidade de seleção/randomização:
--   responsável / telefone
--
-- Regras:
--   - cada CPF responsável aparece no máximo uma vez;
--   - cada celular_disparo aparece no máximo uma vez;
--   - se um responsável tiver várias crianças irregulares,
--     uma criança é escolhida deterministicamente;
--   - exclui a 15ª RA;
--   - remove quem já recebeu a mesma HSM no cooldown;
--   - seleciona até 1.000 responsáveis/telefones.
--
-- Seed: 28031994
-- Saída: tabela pronta para exportação em CSV
-- ============================================================

WITH params AS (
  SELECT
    1000 AS sample_n,
    '28031994' AS seed,

    -- TROCAR pelo templateId real da HSM
    999999 AS id_hsm,

    -- Janela para impedir reenvio da mesma HSM
    30 AS cooldown_days
),

-- ============================================================
-- 1) PROTOCOLOS IRREGULARES
--
-- Identifica as crianças classificadas como irregulares no
-- protocolo de atualização do CadÚnico.
-- ============================================================

protocolo_irregular AS (
  SELECT DISTINCT
    CAST(id_membro_familia AS STRING)
      AS id_membro_familia,

    LPAD(
      REGEXP_REPLACE(
        CAST(cpf AS STRING),
        r'\D',
        ''
      ),
      11,
      '0'
    ) AS cpf_crianca_protocolo,

    nome
      AS nome_crianca_protocolo,

    protocolo_id,
    protocolo_descricao,
    protocolo_level

  FROM
    `rj-crm-registry.intermediario_projeto_pequenos_cariocas.protocolo_smas_cadunico_atualizado`

  WHERE protocolo_status = 'irregular'
),

-- ============================================================
-- 2) PARTICIPANTES ATIVOS DO PIC
--
-- Recupera os dados da criança, da família e um telefone
-- alternativo existente no cadastro do participante.
--
-- O filtro p.pic.status = 'ativo' garante que somente crianças
-- atualmente ativas no programa entrem na base.
-- ============================================================

participantes AS (
  SELECT
    CAST(p.id_membro_familia AS STRING)
      AS id_membro_familia,

    CAST(
      p.assistencia_social.cadunico.id_familia
      AS STRING
    ) AS id_familia,

    LPAD(
      REGEXP_REPLACE(
        CAST(p.cpf AS STRING),
        r'\D',
        ''
      ),
      11,
      '0'
    ) AS cpf_crianca_pic,

    p.nome
      AS nome_crianca_pic,

    p.assistencia_social.cadunico.responsavel_familiar.nome
      AS nome_responsavel_pic,

    COALESCE(
      p.obito.indicador,
      FALSE
    ) AS obito_indicador_crianca,

    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
      CONCAT(
        IFNULL(
          p.telefone.principal.ddi,
          '55'
        ),
        IFNULL(
          p.telefone.principal.ddd,
          '21'
        ),
        p.telefone.principal.valor
      )
    ) AS telefone_pic_formatado

  FROM
    `rj-crm-registry.projeto_pequenos_cariocas.participantes` AS p

  WHERE p.id_membro_familia IS NOT NULL

    -- Somente participantes ativos no PIC
    AND p.pic.status = 'ativo'
),

-- ============================================================
-- 2.1) ENDEREÇO DA FAMÍLIA
--
-- Recupera um bairro para cada id_familia e produz uma versão
-- normalizada para comparação com a lista da 15ª RA.
-- ============================================================

cadunico_endereco AS (
  SELECT
    CAST(id_familia AS STRING)
      AS id_familia,

    UPPER(bairro)
      AS bairro_cadunico,

    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          NORMALIZE_AND_CASEFOLD(
            bairro,
            NFKD
          ),
          r'\p{M}',
          ''
        ),
        r'[\W_]+',
        ' '
      )
    ) AS bairro_cadunico_norm

  FROM
    `rj-crm-registry.app_pequenos_cariocas.endpoint_participante_listagem`

  WHERE id_familia IS NOT NULL
    AND bairro IS NOT NULL

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(id_familia AS STRING)
    ORDER BY bairro
  ) = 1
),

-- ============================================================
-- 3) RESPONSÁVEL FAMILIAR NO RMI
--
-- Seleciona um responsável elegível por família:
--   - maior de idade;
--   - não falecido;
--   - telefone válido;
--   - estratégia ENVIAR ou TESTAR;
--   - celular;
--   - sem opt-out;
--   - sem quarentena.
-- ============================================================

responsavel_rmi AS (
  SELECT
    CAST(
      pf.assistencia_social.cadunico.id_familia
      AS STRING
    ) AS id_familia,

    LPAD(
      REGEXP_REPLACE(
        CAST(pf.cpf AS STRING),
        r'\D',
        ''
      ),
      11,
      '0'
    ) AS cpf_responsavel,

    COALESCE(
      pf.nome_social,
      pf.nome
    ) AS nome_responsavel,

    -- Mantido para auditoria/comparação com o bairro do CadÚnico
    pf.endereco.principal.bairro
      AS bairro_responsavel_rmi,

    pf.sexo
      AS sexo_responsavel,

    DATE_DIFF(
      CURRENT_DATE('America/Sao_Paulo'),
      DATE(pf.nascimento.data),
      YEAR
    ) AS idade_responsavel,

    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
      CONCAT(
        IFNULL(
          pf.telefone.principal.ddi,
          '55'
        ),
        IFNULL(
          pf.telefone.principal.ddd,
          '21'
        ),
        pf.telefone.principal.valor
      )
    ) AS celular_disparo,

    pf.telefone.principal.estrategia_envio
      AS estrategia_envio,
    pf.telefone.principal.qualidade as qualidade,
    pf.telefone.principal.confianca as confianca,

    CASE
      WHEN pf.telefone.principal.estrategia_envio = 'ENVIAR'
        THEN 0

      WHEN pf.telefone.principal.estrategia_envio = 'TESTAR'
        THEN 1

      ELSE 2
    END AS phone_priority

  FROM
    `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS pf

  WHERE
    pf.assistencia_social.cadunico.id_familia IS NOT NULL

    -- Identifica o responsável familiar
    AND pf.assistencia_social.cadunico
          .responsavel_familiar
          .parentesco_com_responsavel IS NULL

    -- Filtros obrigatórios de elegibilidade para contato
    AND pf.menor_idade IS FALSE

    AND pf.obito.indicador IS FALSE

    AND pf.telefone.principal.qualidade = 'VALIDO'

    AND pf.telefone.principal.estrategia_envio
        IN ('ENVIAR', 'TESTAR')

    AND pf.telefone.principal.tipo = 'CELULAR'

    AND pf.telefone.principal.valor IS NOT NULL

    AND COALESCE(
      pf.telefone.principal.indicador_optout,
      FALSE
    ) = FALSE

    AND COALESCE(
      pf.telefone.principal.indicador_quarentena,
      FALSE
    ) = FALSE

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(
      pf.assistencia_social.cadunico.id_familia
      AS STRING
    )

    ORDER BY
      CASE
        WHEN pf.telefone.principal.estrategia_envio = 'ENVIAR'
          THEN 0
        ELSE 1
      END,

      pf.cpf
  ) = 1
),

-- ============================================================
-- 4) BAIRROS DA 15ª RA
--
-- Lista os nomes oficiais, abreviações e erros de digitação
-- previstos para os bairros que devem ser excluídos.
-- ============================================================

bairros_15ra AS (
  SELECT DISTINCT
    bairro_original,

    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          NORMALIZE_AND_CASEFOLD(
            bairro_original,
            NFKD
          ),
          r'\p{M}',
          ''
        ),
        r'[\W_]+',
        ' '
      )
    ) AS bairro_norm

  FROM UNNEST([
    -- Bento Ribeiro
    'Bento Ribeiro',
    'Bento Ribero',
    'Bento Ribeiros',
    'Bto Ribeiro',

    -- Campinho
    'Campinho',
    'Campimho',

    -- Cascadura
    'Cascadura',
    'Cascadoura',

    -- Cavalcanti
    'Cavalcanti',
    'Cavalcante',
    'Cavalcanti RJ',

    -- Engenheiro Leal
    'Engenheiro Leal',
    'Eng Leal',
    'Eng. Leal',
    'Engenheiro Lial',

    -- Honório Gurgel
    'Honório Gurgel',
    'Honorio Gurgel',
    'Honório Gurguel',
    'Honorio Gurguel',

    -- Madureira
    'Madureira',
    'Madureyra',

    -- Marechal Hermes
    'Marechal Hermes',
    'Mal Hermes',
    'Mal. Hermes',
    'Marechal Hermez',

    -- Osvaldo / Oswaldo Cruz
    'Osvaldo Cruz',
    'Oswaldo Cruz',
    'Osvaldo Crux',
    'Oswaldo Crux',

    -- Quintino Bocaiúva
    'Quintino Bocaiúva',
    'Quintino Bocaiuva',
    'Quintino Bocaiúba',
    'Quintino Bocaiuba',
    'Quintino',

    -- Rocha Miranda
    'Rocha Miranda',
    'Roxa Miranda',
    'Rocha Miramda',

    -- Turiaçú
    'Turiaçú',
    'Turiaçu',
    'Turiacu',
    'Turyacu',
    'Turiassu',

    -- Vaz Lobo
    'Vaz Lobo',
    'Vas Lobo',
    'Vazlobo'
  ]) AS bairro_original
),

-- ============================================================
-- 5) BASE ELEGÍVEL ANTES DA EXCLUSÃO TERRITORIAL
--
-- Combina protocolo, criança ativa, responsável e endereço.
-- ============================================================

base_elegivel_pre AS (
  SELECT
    pr.id_membro_familia,

    p.id_familia,

    COALESCE(
      pr.cpf_crianca_protocolo,
      p.cpf_crianca_pic
    ) AS cpf_crianca,

    COALESCE(
      pr.nome_crianca_protocolo,
      p.nome_crianca_pic
    ) AS nome_crianca,

    r.cpf_responsavel,

    COALESCE(
      r.nome_responsavel,
      p.nome_responsavel_pic
    ) AS nome_responsavel,

    ce.bairro_cadunico
      AS bairro,

    ce.bairro_cadunico_norm
      AS bairro_norm,

    r.bairro_responsavel_rmi,

    r.sexo_responsavel,
    r.idade_responsavel,

    r.celular_disparo,
    r.phone_priority,
    r.estrategia_envio,
    r.qualidade,
    r.confianca,

    p.telefone_pic_formatado,

    pr.protocolo_id,
    pr.protocolo_descricao,
    pr.protocolo_level

  FROM protocolo_irregular AS pr

  INNER JOIN participantes AS p
    ON pr.id_membro_familia = p.id_membro_familia

  INNER JOIN responsavel_rmi AS r
    ON p.id_familia = r.id_familia

  LEFT JOIN cadunico_endereco AS ce
    ON p.id_familia = ce.id_familia

  WHERE
    COALESCE(
      p.obito_indicador_crianca,
      FALSE
    ) = FALSE

    AND ce.bairro_cadunico_norm IS NOT NULL

    AND r.celular_disparo IS NOT NULL
),

-- ============================================================
-- 5.1) EXCLUI A 15ª RA
--
-- Faz correspondência exata após normalização ou correspondência
-- aproximada para pequenos erros de digitação.
-- ============================================================

base_elegivel AS (
  SELECT
    b.*

  FROM base_elegivel_pre AS b

  LEFT JOIN bairros_15ra AS br
    ON (
      b.bairro_norm = br.bairro_norm

      OR (
        SUBSTR(
          b.bairro_norm,
          1,
          1
        ) = SUBSTR(
          br.bairro_norm,
          1,
          1
        )

        AND ABS(
          LENGTH(b.bairro_norm)
          - LENGTH(br.bairro_norm)
        ) <= 2

        AND EDIT_DISTANCE(
          b.bairro_norm,
          br.bairro_norm
        ) <= 2
      )
    )

  WHERE br.bairro_norm IS NULL
),

-- ============================================================
-- 6) REMOVE QUEM JÁ RECEBEU A MESMA HSM NO COOLDOWN
--
-- A verificação é feita pelo CPF do responsável.
-- ============================================================

filtra_disparados AS (
  SELECT
    b.*

  FROM base_elegivel AS b

  CROSS JOIN params AS prm

  LEFT JOIN
    `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` AS fl

    ON LPAD(
         REGEXP_REPLACE(
           CAST(fl.targetexternalid AS STRING),
           r'\D',
           ''
         ),
         11,
         '0'
       ) = b.cpf_responsavel

   AND fl.templateId = prm.id_hsm

   AND DATE(fl.createDate) >= DATE_SUB(
     CURRENT_DATE('America/Sao_Paulo'),
     INTERVAL prm.cooldown_days DAY
   )

  WHERE fl.targetexternalid IS NULL
),

-- ============================================================
-- 7) ESCOLHE UMA CRIANÇA POR RESPONSÁVEL
--
-- Quando um responsável está associado a mais de uma criança
-- irregular, seleciona deterministicamente apenas uma criança.
-- ============================================================

dedup_responsavel AS (
  SELECT
    f.*

  FROM filtra_disparados AS f

  CROSS JOIN params AS prm

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY f.cpf_responsavel

    ORDER BY
      f.phone_priority,

      FARM_FINGERPRINT(
        CONCAT(
          CAST(
            f.id_membro_familia
            AS STRING
          ),
          '|representative_child|seed:',
          prm.seed
        )
      ),

      CAST(
        f.id_membro_familia
        AS STRING
      ),

      COALESCE(
        CAST(f.protocolo_id AS STRING),
        ''
      )
  ) = 1
),

-- ============================================================
-- 7.1) GARANTE UM ÚNICO RESPONSÁVEL POR TELEFONE
--
-- Se o mesmo telefone estiver associado a CPFs diferentes,
-- seleciona deterministicamente apenas um responsável.
-- ============================================================

dedup_telefone AS (
  SELECT
    d.*

  FROM dedup_responsavel AS d

  CROSS JOIN params AS prm

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.celular_disparo

    ORDER BY
      d.phone_priority,

      FARM_FINGERPRINT(
        CONCAT(
          CAST(
            d.cpf_responsavel
            AS STRING
          ),
          '|telephone_owner|seed:',
          prm.seed
        )
      ),

      d.cpf_responsavel
  ) = 1
),

-- ============================================================
-- 8) RANDOMIZAÇÃO NO NÍVEL RESPONSÁVEL / TELEFONE
--
-- Neste ponto, cada CPF responsável e cada celular_disparo
-- aparecem no máximo uma vez.
-- ============================================================

sorteio AS (
  SELECT
    d.*,

    CONCAT(
      CAST(
        d.cpf_responsavel
        AS STRING
      ),
      '|',
      CAST(
        d.celular_disparo
        AS STRING
      )
    ) AS unidade_randomizacao,

    ROW_NUMBER() OVER (
      ORDER BY
        FARM_FINGERPRINT(
          CONCAT(
            CAST(
              d.cpf_responsavel
              AS STRING
            ),
            '|',
            CAST(
              d.celular_disparo
              AS STRING
            ),
            '|randomization|seed:',
            prm.seed
          )
        ),

        d.cpf_responsavel,
        d.celular_disparo
    ) AS ordem_sorteio

  FROM dedup_telefone AS d

  CROSS JOIN params AS prm
),

-- ============================================================
-- 8.1) SELECIONA ATÉ 1.000 RESPONSÁVEIS / TELEFONES
-- ============================================================

amostra AS (
  SELECT
    s.*

  FROM sorteio AS s

  CROSS JOIN params AS prm

  WHERE s.ordem_sorteio <= prm.sample_n
),

-- ============================================================
-- 9) TELEFONES ALTERNATIVOS
--
-- Como a saída é CSV, os telefones alternativos são reunidos
-- em uma string separada por ponto e vírgula.
-- ============================================================

amostra_com_others AS (
  SELECT
    *,

    ARRAY_TO_STRING(
      ARRAY(
        SELECT DISTINCT tel

        FROM UNNEST([
          telefone_pic_formatado
        ]) AS tel

        WHERE tel IS NOT NULL
          AND tel != celular_disparo
      ),
      ';'
    ) AS others

  FROM amostra
)

-- ============================================================
-- 10) SAÍDA FINAL TABULAR / CSV
--
-- Cada linha representa uma unidade responsável/telefone.
-- ============================================================

SELECT
  -- Ordenação e unidade de seleção
  ordem_sorteio,
  unidade_randomizacao,

  -- Identificadores do responsável
  CAST(
    cpf_responsavel
    AS STRING
  ) AS SubscriberKey,

  -- Telefones
  celular_disparo as telefone,
  others,

  -- Dados formatados para os parâmetros da HSM
  `rj-crm-registry.udf.FORMAT_NAME`(
    nome_responsavel,
    TRUE
  ) AS nome,

  `rj-crm-registry.udf.FORMAT_NAME`(
    nome_crianca,
    TRUE
  ) AS crianca,

  protocolo_descricao
    AS PROTOCOLO,

  CAST(
    id_membro_familia
    AS STRING
  ) AS id_crianca,

  bairro

  -- Colunas adicionais para auditoria
  id_familia,
  cpf_crianca,
  nome_responsavel,
  nome_crianca,

  bairro_norm,
  bairro_responsavel_rmi,

  sexo_responsavel,
  idade_responsavel,

  estrategia_envio,
  confianca,
  qualidade,
  phone_priority,

  protocolo_id,
  protocolo_level

FROM amostra_com_others

-- Mantém a regra original de não gerar disparos em fins de semana
WHERE EXTRACT(
        DAYOFWEEK
        FROM CURRENT_DATE('America/Sao_Paulo')
      ) NOT IN (1, 7)
and celular_disparo is not null and cpf_responsavel is not null

ORDER BY ordem_sorteio;

