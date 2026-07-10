-- Teste da query queries_dev/pgm_divida_ativa_cobranca.sql (mirror de queries/pgm_divida_ativa_cobranca.sql)
-- Placeholders já substituídos com os valores reais do schedule "daily-pgm-divida-ativa-prod"
-- (scheduler_sf.yaml): nome_hsm_cobranca_placeholder -> 'pgm_divida_ativa_prod_v2',
-- limit_placeholder -> 300.
-- Executa direto no BigQuery. Passos 1 e 2 (abaixo) inserem um CPF de teste que cai em
-- todos os filtros da query; passo 3 é a query em si, adaptada dos datasets dev__dev_fantasma__.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
WHERE cpf = '12pgmcobra1';

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
WHERE cpf_cnpj = '12pgmcobra1';

-- garante que o cpf de teste não aparece como já disparado (filtra_disparados exige sd.cpf IS NULL)
DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12pgmcobra1'
    AND nome_hsm = 'pgm_divida_ativa_prod_v2';

-- ===================== 1) INSERT: pessoa física de teste (RMI) =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
(cpf, nome, nome_social, nascimento, menor_idade, obito, telefone)
VALUES
(
    '12pgmcobra1',
    'MARIA SALESFORCE COBRANCA',
    NULL,
    STRUCT(
        DATE '1993-04-17' AS data,
        CAST(NULL AS STRING) AS municipio_id,
        CAST(NULL AS STRING) AS municipio,
        CAST(NULL AS STRING) AS uf,
        CAST(NULL AS STRING) AS pais_id,
        CAST(NULL AS STRING) AS pais
    ),
    FALSE,
    STRUCT(FALSE AS indicador, NULL AS ano),
    STRUCT(
        TRUE AS indicador,
        STRUCT(
            'TESTE' AS origem,
            'TESTE' AS sistema,
            '55' AS ddi,
            '21' AS ddd,
            '989190512' AS valor,
            'VALIDO' AS qualidade,
            CAST(NULL AS STRING) AS confianca,
            'CELULAR' AS tipo,
            'ENVIAR' AS estrategia_envio,
            FALSE AS indicador_optin,
            FALSE AS indicador_optout,
            FALSE AS indicador_soft_optin,
            FALSE AS indicador_quarentena,
            CAST(NULL AS DATE) AS data_fim_quarentena,
            CAST(NULL AS STRING) AS consentimento,
            CAST(NULL AS STRING) AS razao_optout,
            CAST(NULL AS DATETIME) AS datahora_optin,
            CAST(NULL AS DATETIME) AS datahora_optout,
            CAST(NULL AS DATETIME) AS datahora_ultima_leitura,
            CAST(NULL AS DATETIME) AS datahora_ultima_resposta,
            CURRENT_DATE('America/Sao_Paulo') AS data_atualizacao
        ) AS principal,
        CAST([] AS ARRAY<STRUCT<origem STRING, sistema STRING, ddi STRING, ddd STRING, valor STRING, qualidade STRING, confianca STRING, tipo STRING, estrategia_envio STRING, indicador_optin BOOL, indicador_optout BOOL, indicador_soft_optin BOOL, indicador_quarentena BOOL, data_fim_quarentena DATE, consentimento STRING, razao_optout STRING, datahora_optin DATETIME, datahora_optout DATETIME, datahora_ultima_leitura DATETIME, datahora_ultima_resposta DATETIME, data_atualizacao DATE>>) AS alternativo
    )
);

-- ===================== 2) INSERT: contribuinte de dívida ativa de teste =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
(id_pessoa, cpf_cnpj, tipo_pessoa, nome, saldo_devido_cdas, valor_cotas_devidas_vencidas, cdas_associadas)
VALUES
(
    999999991,
    '12pgmcobra1',
    STRUCT(1 AS tipo_pessoa, 'Pessoa Física' AS descricao_tipo_pessoa),
    'MARIA SALESFORCE COBRANCA',
    CAST(1000 AS NUMERIC),
    CAST(500 AS NUMERIC), -- > 0 => esta_pagando = FALSE, exigido pelo filtro elegiveis_divida_ativa
    [STRUCT(
        1 AS id_certidao_divida_ativa,
        2024 AS ano_de_inscricao_na_divida,
        DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 MONTH) AS data_geracao_cda,
        CAST(NULL AS DATETIME) AS data_ultima_alteracao_situacao,
        CAST(NULL AS STRING) AS numero_processo_associado,
        CAST(1000 AS NUMERIC) AS valor_original_divida_ativa,
        CAST(1000 AS NUMERIC) AS valor_saldo_devido,
        CAST(0 AS NUMERIC) AS valor_multa_moratoria,
        CAST(0 AS NUMERIC) AS valor_juros_moratorios,
        CAST(0 AS NUMERIC) AS valor_mora,
        CAST(0 AS NUMERIC) AS valor_juros_mora,
        CAST(0 AS NUMERIC) AS valor_pago_principal,
        CAST(0 AS NUMERIC) AS valor_honorarios,
        CAST(0 AS NUMERIC) AS valor_mora_smf_iptu,
        CAST(NULL AS STRUCT<codigo_receita_cda STRING, nome_receita STRING>) AS tipo_receita,
        CAST(NULL AS STRUCT<id_entidade_credora INT64, nome_entidade_credora STRING>) AS entidade_credora,
        CAST(NULL AS STRUCT<codigo_fase_cobranca INT64, nome_fase_cobranca STRING>) AS fase_cobranca,
        STRUCT(1 AS id_situacao_cda, 'Inscrita' AS descricao_situacao_cda) AS situacao_cda,
        CAST(NULL AS STRUCT<id_natureza_divida INT64, nome_natureza_divida STRING>) AS natureza_divida_ativa,
        CAST(NULL AS STRING) AS dados_complementares,
        1 AS quantidade_devedores_cda,
        CAST([] AS ARRAY<STRUCT<id_pessoa INT64, cpf_cnpj STRING, tipo_pessoa INT64, descricao_tipo_pessoa STRING, nome STRING>>) AS devedores_vinculados_cda,
        FALSE AS possui_imovel_associado,
        CAST(NULL AS STRUCT<
            endereco STRUCT<codigo_logradouro INT64, nome_logradouro STRING, numero_porta STRING, complemento_endereco STRING, bairro STRING, cep STRING>,
            tipologia_imovel STRUCT<id_tipologia_imovel INT64, nome_tipologia_imovel STRING>,
            utilizacao_imovel STRUCT<id_utilizacao_imovel INT64, nome_utilizacao_imovel STRING>
        >) AS imovel_associado
    )]
);

-- ===================== 3) QUERY (queries_dev/pgm_divida_ativa_cobranca.sql com placeholders substituídos) =====================
-- ===================== 3.1) MATERIALIZA =====================
-- ===================== 3.1.1) CELULARES VÁLIDOS (PF) E *MÓVEIS* =====================
CREATE TEMP TABLE tmp_grupos AS

WITH celulares_validos AS (
SELECT
    cpf,
    COALESCE(if(nome_social = "", null, nome_social), nome) AS nome_rmi,
    pf.obito.indicador AS obito_indicador,
    nascimento.data AS dt_nasc,
    DATE_DIFF(CURRENT_DATE("America/Sao_Paulo"), nascimento.data, YEAR) AS idade,
    `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(
    CONCAT(IFNULL(telefone.principal.ddi, '55'), telefone.principal.ddd, telefone.principal.valor)
    ) AS celular_disparo,
    CASE
    WHEN pf.telefone.principal.estrategia_envio = "ENVIAR" THEN 0
    WHEN pf.telefone.principal.estrategia_envio = "TESTAR" THEN 1
    WHEN pf.telefone.principal.estrategia_envio = "EVITAR" THEN 2
    ELSE 3
    END AS phone_priority,
    pf.telefone.principal.estrategia_envio,
    pf.telefone.principal.qualidade
FROM `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica` pf
WHERE pf.telefone.principal.qualidade = 'VALIDO'
    AND pf.telefone.principal.estrategia_envio != "NÃO ENVIAR"
    AND pf.telefone.principal.estrategia_envio in ("ENVIAR", "TESTAR", "EVITAR")
    AND pf.telefone.principal.tipo = "CELULAR"
    AND pf.telefone.principal.valor IS NOT NULL
    AND pf.menor_idade IS FALSE
    AND pf.obito.indicador IS FALSE
    AND nascimento.data IS NOT NULL
    AND EXTRACT(DAYOFWEEK FROM current_date("America/Sao_Paulo")) NOT IN (1, 7)
),
filtra_disparados AS (
-- verifica se esse cpf já recebeu essa mesma mensagem nos últimos x dias
SELECT celulares_validos.*
FROM celulares_validos
LEFT JOIN `rj-crm-registry.brutos_salesforce.status_disparo` sd
    ON sd.cpf = celulares_validos.cpf
    AND sd.nome_hsm = 'pgm_divida_ativa_prod_v2'
    AND sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 150 DAY)
    AND sd.data_particao >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 151 DAY)
WHERE sd.cpf IS NULL
),

contribuintes AS (
-- filtra PF e nome antes do unnest
SELECT
    LPAD(CAST(p.cpf_cnpj AS STRING), 11, '0') AS cpf,
    coalesce(nome_rmi, nome) as nome,
    cdas_associadas,
    tipo_pessoa,
    saldo_devido_cdas,
    valor_cotas_devidas_vencidas
FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte` p
INNER JOIN filtra_disparados
    ON LPAD(CAST(p.cpf_cnpj AS STRING), 11, '0') = filtra_disparados.cpf
WHERE p.tipo_pessoa.descricao_tipo_pessoa = 'Pessoa Física'
    AND p.saldo_devido_cdas > 0
    AND NOT STARTS_WITH(nome, 'ESPÓLIO')
),

guia_pagamento_latest AS (
SELECT
    cpf,
    p.nome,
    cdas_associadas.id_certidao_divida_ativa AS id_certidao_divida_ativa,
    cdas_associadas.data_geracao_cda AS data_geracao_cda,
    cdas_associadas.situacao_cda.descricao_situacao_cda AS descricao_situacao_cda,
    cdas_associadas.valor_saldo_devido AS valor_saldo_devido,
    p.valor_cotas_devidas_vencidas,

    CASE
    WHEN p.valor_cotas_devidas_vencidas > 0
        OR p.valor_cotas_devidas_vencidas IS NULL
    THEN FALSE
    ELSE TRUE
    END AS esta_pagando
FROM contribuintes p
LEFT JOIN UNNEST(
    ARRAY(
    SELECT c
    FROM UNNEST(p.cdas_associadas) c
    WHERE c.situacao_cda.descricao_situacao_cda IN ('Inscrita','Cobrança')
    )
) AS cdas_associadas
WHERE cdas_associadas.situacao_cda.descricao_situacao_cda IN ('Inscrita','Cobrança')
),

-- ===================== 3.2) BASE COM PAGAMENTOS =====================
-- soma por cpf e cda primeiro, já que temos mais de uma linha por cda
base_ AS (
SELECT
    cpf,
    MAX(nome) AS nome,
    id_certidao_divida_ativa,
    data_geracao_cda,
    valor_saldo_devido,
    MIN(esta_pagando) AS esta_pagando  --pode ser que esteja pagando uma CDA e outra não
FROM guia_pagamento_latest
GROUP BY
    cpf,
    id_certidao_divida_ativa,
    data_geracao_cda,
    valor_saldo_devido
),

-- ===================== 3.3) UMA LINHA POR CPF =====================
per_cpf AS (
SELECT
    b.cpf,
    MAX(cm.dt_nasc) AS dt_nasc,
    MAX(cm.idade) AS idade,
    MAX(cm.nome_rmi) AS nome,
    MAX(cm.celular_disparo) AS celular_disparo,
    MAX(cm.phone_priority) AS phone_priority,
    MAX(b.data_geracao_cda) AS dt_ultima_cda,
    COUNT(b.id_certidao_divida_ativa) AS n_cdas_ativas,
    SUM(b.valor_saldo_devido) AS soma_divida_total,
    MIN(esta_pagando) AS esta_pagando
FROM base_ b
JOIN filtra_disparados cm USING (cpf)
GROUP BY 1
),

elegiveis_divida_ativa AS (
SELECT *
FROM per_cpf
WHERE soma_divida_total <= 6000
    AND esta_pagando = FALSE
    AND idade <= 80
    AND dt_ultima_cda >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 MONTH)
),

-- ===================== 3.4) RANDOMIZAÇÃO DETERMINÍSTICA =====================
randomizados AS (
SELECT *,
    ROW_NUMBER() OVER (
    ORDER BY ABS(FARM_FINGERPRINT(CONCAT(CAST(cpf AS STRING), '|seed:17041993')))
    ) AS ordem_sorteio
FROM elegiveis_divida_ativa
),

-- ===================== 3.5) ATRIBUIÇÃO A GRUPOS ESCALONADOS =====================
grupos_escalonados AS (
SELECT
    *,
    CAST(
    CEIL((SQRT(22500 + 200 * ordem_sorteio) - 150) / 100)
    AS INT64
    ) AS grupo_escalonado,
    100 * (
    CAST(
        CEIL((SQRT(22500 + 200 * ordem_sorteio) - 150) / 100)
        AS INT64
    ) + 1
    ) AS tamanho_alvo_grupo
FROM randomizados
)

SELECT * FROM grupos_escalonados;

-- ===================== 4) INSERT: tabela de teste ab =====================
-- Mantido comentado igual à versão dev: `rj-crm-registry.ab_test.divida_ativa` é tabela
-- de PRODUÇÃO (bucketing real do teste A/B). Não gravar dados de teste lá.
-- INSERT INTO `rj-crm-registry.ab_test.divida_ativa`
-- SELECT
-- cpf,
-- celular_disparo,
-- nome,
-- grupo_escalonado,
-- ordem_sorteio,
-- tamanho_alvo_grupo,
-- soma_divida_total,
-- n_cdas_ativas,
-- esta_pagando,
-- idade,
-- CURRENT_TIMESTAMP(),
-- cast(cpf as int) as cpf_particao
-- FROM tmp_grupos;

-- ===================== 5) Estrutura dados para o disparo =====================
-- Tabela simples (sem TO_JSON_STRING). 'externalId' é controle interno (dedup por
-- CPF) e é descartado do CSV pelo de_columns antes do envio à Data Extension.
SELECT
    celular_disparo AS telefone,
    CAST(cpf AS STRING) AS SubscriberKey,
    CAST(cpf AS STRING) AS externalId,
    INITCAP(
        IF(
            ARRAY_LENGTH(SPLIT(nome, ' ')) > 1,
            CONCAT(
                SPLIT(nome, ' ')[SAFE_OFFSET(0)], ' ',
                SPLIT(nome, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(nome, ' ')) - 1)]
            ),
            nome
        )
    ) AS nome_sobrenome
FROM tmp_grupos
ORDER BY ordem_sorteio
LIMIT cast(300 as int64);
