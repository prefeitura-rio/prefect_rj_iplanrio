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
        -- data_hora vem como STRING na tabela de origem, precisa de CAST explícito pra DATETIME
        FORMAT_DATETIME('%d/%m/%Y', CAST(data_hora AS DATETIME)) AS data,
        FORMAT_DATETIME('%H:%M', CAST(data_hora AS DATETIME)) AS hora
    FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`
    WHERE
        DATE(CAST(data_hora AS DATETIME)) = DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL CAST({days_ahead_placeholder} AS int64) DAY)
),

filtra_disparados AS (
    -- verifica se esse cpf já recebeu essa mesma mensagem (nome_hsm_placeholder) nos últimos x dias,
    -- tanto via status_disparo quanto via telefone_disparado (tabela legada pré-migração,
    -- por telefone; id_hsm 101 = HSM de confirmação de agendamento do CadÚnico)
    SELECT segmentacao_original.*
    FROM segmentacao_original
    LEFT JOIN `rj-crm-registry.brutos_salesforce.status_disparo` sd
        ON sd.cpf = segmentacao_original.cpf
        AND sd.nome_hsm = '{nome_hsm_placeholder}'
        AND sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL {intervalo_filtro_disparados} DAY)
        AND sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL {intervalo_filtro_disparados} DAY)
        AND sd.indicador_quarentena = FALSE
    LEFT JOIN `rj-crm-registry.crm_whatsapp.telefone_disparado` td
        ON td.contato_telefone = segmentacao_original.celular_disparo
        AND td.id_hsm = CAST({id_hsm_legado_placeholder} AS STRING)
        AND td.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL {intervalo_filtro_disparados} DAY)
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

-- Tabela simples (sem TO_JSON_STRING). 'externalId' é controle interno (dedup por
-- CPF) e é descartado do CSV pelo de_columns antes do envio à Data Extension.
-- telefone, SubscriberKey e LOCALE já são tratados por padrão pelo flow (LOCALE é
-- preenchido automaticamente como 'BR' em save_csv_for_sftp).
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
