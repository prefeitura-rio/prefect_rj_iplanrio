-- ===================== 1) MATERIALIZA =====================
-- ===================== 1.1) CELULARES VÁLIDOS (PF) E *MÓVEIS* =====================
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
    AND sd.nome_hsm = '{nome_hsm_cobranca_placeholder}'
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

-- ===================== 1.2) BASE COM PAGAMENTOS =====================
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

-- ===================== 1.3) UMA LINHA POR CPF =====================
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

-- ===================== 1.4) RANDOMIZAÇÃO DETERMINÍSTICA =====================
randomizados AS (
SELECT *,
    ROW_NUMBER() OVER (
    ORDER BY ABS(FARM_FINGERPRINT(CONCAT(CAST(cpf AS STRING), '|seed:17041993')))
    ) AS ordem_sorteio
FROM elegiveis_divida_ativa
),

-- ===================== 1.5) ATRIBUIÇÃO A GRUPOS ESCALONADOS =====================
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

-- ===================== 2) INSERT: Insere dados da tabela de teste ab =====================
-- Comentado na versão dev: `rj-crm-registry.ab_test.divida_ativa` é tabela de PRODUÇÃO
-- (bucketing real do teste A/B). Não gravar dados de teste lá. Descomente só se/quando
-- existir um mirror dev dessa tabela.
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

-- ===================== 3) Estrutura dados para o disparo =====================
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
LIMIT cast({limit_placeholder} as int64);
