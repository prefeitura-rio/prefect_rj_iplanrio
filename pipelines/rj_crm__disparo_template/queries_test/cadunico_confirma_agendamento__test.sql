-- Teste da query queries_dev/cadunico_confirma_agendamento.sql (mirror de queries/cadunico_confirma_agendamento.sql)
-- Placeholders já substituídos com valores de exemplo do schedule "daily-cadunico-confirma-agendamento"
-- (scheduler_sf.yaml): days_ahead_placeholder -> 2, nome_hsm_placeholder -> 'confirma_agendamento_cadunico_prod_v2',
-- intervalo_filtro_disparados -> 15.
-- Executa direto no BigQuery. Passo 1 limpa dados de teste anteriores; passo 2 insere um
-- agendamento CadÚnico marcado para daqui a 2 dias (exigido pelo filtro de data da query),
-- sem nenhum disparo anterior para esse CPF, caindo assim em todos os filtros; passo 3 é a query em si.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`
WHERE cpf = '12cadunico0';

DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12cadunico0'
    AND nome_hsm = 'confirma_agendamento_cadunico_prod_v2';

-- telefone_disparado não tem cpf, só contato_telefone (o mesmo número usado no passo 1);
-- sem esse delete, um disparo real anterior para esse telefone (id_hsm 101) derruba o teste.
DELETE FROM `rj-crm-registry.crm_whatsapp.telefone_disparado`
WHERE contato_telefone = '5521989190512'
    AND id_hsm = '101';

-- ===================== 1) INSERT: agendamento CadÚnico de teste =====================
-- data_hora é STRING na tabela de origem (formato canônico DATETIME 'YYYY-MM-DD HH:MM:SS')
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`
(id, telefone, nome_completo, data_hora, unidade_nome, unidade_endereco, unidade_bairro, cpf)
VALUES
(
    '999999993',
    '5521989190512',
    'MARIA SALESFORCE CADUNICO',
    CAST(DATETIME_ADD(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 DAY) AS STRING),
    'CRAS Centro',
    'Rua das Flores 123',
    'Centro',
    '12cadunico0'
);

-- ===================== 2) QUERY (queries_dev/cadunico_confirma_agendamento.sql com placeholders substituídos) =====================
WITH
segmentacao_original AS (
    SELECT
        LPAD(CAST(cpf AS STRING), 11, '0') AS cpf,
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(telefone, r'[^\d]', '')) AS celular_disparo,
        INITCAP(
            IF(
                ARRAY_LENGTH(SPLIT(nome_completo, ' ')) > 1,
                CONCAT(
                    SPLIT(nome_completo, ' ')[SAFE_OFFSET(0)],
                    ' ',
                    SPLIT(nome_completo, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(nome_completo, ' ')) - 1)]
                ),
                nome_completo
            )
        ) AS nome_sobrenome,
        unidade_nome AS unidade_cras,
        CONCAT(unidade_endereco, ' - ', unidade_bairro) AS endereco,
        FORMAT_DATETIME('%d/%m/%Y', CAST(data_hora AS DATETIME)) AS data,
        FORMAT_DATETIME('%H:%M', CAST(data_hora AS DATETIME)) AS hora
    FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`
    WHERE
        DATE(CAST(data_hora AS DATETIME)) = DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL CAST(2 AS int64) DAY)
),

filtra_disparados AS (
    -- verifica se esse cpf já recebeu essa mesma mensagem (confirma_agendamento_cadunico_prod_v2) nos últimos x dias,
    -- tanto via status_disparo quanto via telefone_disparado (tabela legada pré-migração,
    -- por telefone; id_hsm 101). O passo 0 limpa esse telefone na telefone_disparado para
    -- que disparos reais anteriores não derrubem o teste; o LEFT JOIN fica inerte por isso,
    -- não por falta de dados na tabela.
    SELECT segmentacao_original.*
    FROM segmentacao_original
    LEFT JOIN `rj-crm-registry.brutos_salesforce.status_disparo` sd
        ON sd.cpf = segmentacao_original.cpf
        AND sd.nome_hsm = 'confirma_agendamento_cadunico_prod_v2'
        AND sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 15 DAY)
        AND sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
        AND sd.indicador_quarentena = FALSE
    LEFT JOIN `rj-crm-registry.crm_whatsapp.telefone_disparado` td
        ON td.contato_telefone = segmentacao_original.celular_disparo
        AND td.id_hsm = CAST(101 AS STRING)
        AND td.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
    WHERE sd.cpf IS NULL AND td.contato_telefone IS NULL
),

filtra_celulares_sem_whats AS (
    -- remove telefones em quarentena ou com qualidade inválida
    SELECT
        DISTINCT s.*
    FROM filtra_disparados AS s
    LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__intermediario_rmi_telefones.int_telefone` AS tel
        ON s.celular_disparo = tel.telefone_numero_completo
    LEFT JOIN UNNEST(tel.consentimento) AS c
    WHERE
        (c.indicador_quarentena = FALSE AND tel.telefone_qualidade != "INVALIDO")
        OR tel.telefone_numero_completo IS NULL
)

SELECT
    celular_disparo AS telefone,
    cpf AS SubscriberKey,
    cpf AS externalId,
    nome_sobrenome,
    unidade_cras,
    data,
    hora,
    endereco
FROM filtra_celulares_sem_whats
WHERE celular_disparo IS NOT NULL
