-- Teste da query queries_dev/smas_cartaopic_lembrete.sql (mirror de queries/smas_cartaopic_lembrete.sql)
-- Placeholders já substituídos com os valores do schedule "daily-smas-cartaopic-lembrete"
-- (scheduler_sf.yaml): nome_hsm_placeholder -> 'smascartaoprimeirainfancialembreteprodv4',
-- intervalo_filtro_disparados -> 30.
-- Executa direto no BigQuery. Passo 1 aprova o disparo de lembrete de hoje pra uma data de
-- entrega em D+3 (satisfaz o gate DECLARE target_date da query); passo 2 insere um
-- cidadão com entrega prevista pra essa mesma data, telefone válido e status/tipo que
-- passam nos filtros finais; passo 3 é a query em si (com o DECLARE/IF/ELSE embutido).

-- DECLARE precisa ser a primeira instrução do script (regra do BigQuery scripting),
-- então vem antes dos DELETE/INSERT de setup; o valor real só é atribuído no passo 3,
-- via SET, depois que os dados de teste já existem nas tabelas.
DECLARE target_date DATE;

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`
WHERE BAIRRO = 'TESTE SALESFORCE PIC LEMBRETE';

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__pic.cartao_primeira_infancia_carioca_status`
WHERE NUM_CPF_RESPONSAVEL = '12piclembr1';

DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12piclembr1'
    AND nome_hsm = 'smascartaoprimeirainfancialembreteprodv4';

-- ===================== 1) INSERT: aprovação do disparo de lembrete (gate) =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`
(BAIRRO, TIPO_ENTREGA, DATA_ENTREGA, APROVACAO_DISPARO_LEMBRETE, DATA_DISPARO_LEMBRETE)
VALUES
(
    'TESTE SALESFORCE PIC LEMBRETE',
    'Correios',
    DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY),
    'aprovado',
    CURRENT_DATE('America/Sao_Paulo')
);

-- ===================== 2) INSERT: cidadão com entrega prevista pra D+3 =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__pic.cartao_primeira_infancia_carioca_status`
(NUM_CPF_RESPONSAVEL, NOME_RESPONSAVEL, DATA_ENTREGA_PREVISTA, LOCAL_ENTREGA_PREVISTO, ENDERECO_ENTREGA_PREVISTO, HORA_ENTREGA_PREVISTA, NUM_TEL_CONTATO_1_FAM, STATUS, TIPO_ENTREGA_PREVISTA)
VALUES
(
    '12piclembr1',
    'MARIA SALESFORCE PIC LEMBRETE',
    DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY),
    'Posto de Entrega Central',
    'Rua Teste, 123',
    '14:00',
    '5521989190512',
    'aguardando retirada',
    'Correios'
);

-- ===================== 3) QUERY (queries_dev/smas_cartaopic_lembrete.sql com placeholders substituídos) =====================
SET target_date = (
    SELECT DATE(DATA_ENTREGA)
    FROM `rj-crm-registry-dev.dev__dev_fantasma__pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`
    WHERE
        LOWER(TRIM(APROVACAO_DISPARO_LEMBRETE)) = 'aprovado'
        AND DATE(DATA_DISPARO_LEMBRETE) = CURRENT_DATE('America/Sao_Paulo')
        AND TIPO_ENTREGA != 'CRAS'
    LIMIT 1
);

IF target_date IS NULL THEN
    SELECT
        CAST(NULL AS STRING) AS telefone,
        CAST(NULL AS STRING) AS SubscriberKey,
        CAST(NULL AS STRING) AS externalId,
        CAST(NULL AS STRING) AS nome_sobrenome,
        CAST(NULL AS STRING) AS endereco,
        CAST(NULL AS STRING) AS data,
        CAST(NULL AS STRING) AS horario
    LIMIT 0;
ELSE
    WITH
    agendamentos_unicos AS (
        SELECT
            LPAD(cpf, 11, '0') AS cpf,
            telefone,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_hora DESC) AS rn
        FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_data_metrica_staging.cadunico_agendamentos`
        WHERE cpf IS NOT NULL
    ),

    dados_rmi AS (
        SELECT
            cpf,
            telefone
        FROM `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica` AS rmi
        WHERE menor_idade IS FALSE AND obito.indicador IS FALSE
    ),

    telefone_principal_rmi AS (
        SELECT
            cpf,
            CONCAT(IFNULL(telefone.principal.ddi, "55"), telefone.principal.ddd, telefone.principal.valor) AS telefone_principal
        FROM dados_rmi AS rmi
        WHERE telefone.principal.estrategia_envio IN ("ENVIAR", "TESTAR")
    ),

    telefones_alternativos_rmi AS (
        SELECT
            cpf,
            CONCAT(IFNULL(tel_alt.ddi, "55"), tel_alt.ddd, tel_alt.valor) AS telefone_alternativo,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY tel_alt.origem) AS rn
        FROM dados_rmi AS rmi, UNNEST(rmi.telefone.alternativo) AS tel_alt
        WHERE tel_alt.valor IS NOT NULL
    ),

    joined_status_cpi AS (
        SELECT
            TRIM(t1.NOME_RESPONSAVEL) AS nome_completo,
            LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') AS cpf,
            SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) AS data_evento_date,
            TRIM(
                CONCAT(
                    t1.LOCAL_ENTREGA_PREVISTO,
                    IF(
                        t1.ENDERECO_ENTREGA_PREVISTO IS NOT NULL AND t1.ENDERECO_ENTREGA_PREVISTO != '',
                        CONCAT(' (', t1.ENDERECO_ENTREGA_PREVISTO, ')'),
                        ''
                    )
                )
            ) AS endereco_evento,
            t1.HORA_ENTREGA_PREVISTA AS horario_evento,
            COALESCE(
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(t1.NUM_TEL_CONTATO_1_FAM),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(t1.NUM_TEL_CONTATO_2_FAM),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(a.telefone),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(rmi.telefone_principal),
                `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(tel_alt.telefone_alternativo)
            ) AS celular_disparo
        FROM `rj-crm-registry-dev.dev__dev_fantasma__pic.cartao_primeira_infancia_carioca_status` AS t1
        LEFT JOIN agendamentos_unicos AS a
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = a.cpf AND a.rn = 1
        LEFT JOIN telefone_principal_rmi AS rmi
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = rmi.cpf
        LEFT JOIN telefones_alternativos_rmi AS tel_alt
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = tel_alt.cpf AND tel_alt.rn = 1
        WHERE
            SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) = target_date
            AND STATUS != 'não retirado'
            AND TIPO_ENTREGA_PREVISTA != 'CRAS'
    ),

    filtra_disparados AS (
        -- verifica se esse cpf já recebeu essa mesma mensagem (smascartaoprimeirainfancialembreteprodv4) nos últimos 30 dias,
        -- tanto via status_disparo quanto via wetalkie (histórico legado pré-migração;
        -- templateId 185 = HSM de lembrete do PIC). Sem seed na wetalkie aqui de propósito:
        -- o LEFT JOIN não encontra nada e fica inerte, só exercitamos o caminho da status_disparo.
        SELECT joined_status_cpi.*
        FROM joined_status_cpi
        LEFT JOIN `rj-crm-registry.brutos_salesforce.status_disparo` sd
            ON sd.cpf = joined_status_cpi.cpf
            AND sd.nome_hsm = 'smascartaoprimeirainfancialembreteprodv4'
            AND sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 30 DAY)
            AND sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            AND sd.indicador_quarentena = FALSE
        LEFT JOIN `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
            ON fl.flattarget = joined_status_cpi.celular_disparo
            AND fl.templateId = 185
        WHERE sd.cpf IS NULL AND fl.flattarget IS NULL
    ),

    filtra_celulares_sem_whats AS (
        -- remove telefones em quarentena ou com qualidade inválida
        SELECT DISTINCT f.*
        FROM filtra_disparados AS f
        LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__intermediario_rmi_telefones.int_telefone` AS tel
            ON f.celular_disparo = tel.telefone_numero_completo
        LEFT JOIN UNNEST(tel.consentimento) AS c
        WHERE
            (c.indicador_quarentena = FALSE AND tel.telefone_qualidade != "INVALIDO")
            OR tel.telefone_numero_completo IS NULL
    ),

    formatted AS (
        SELECT
            celular_disparo,
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
            cpf,
            endereco_evento,
            FORMAT_DATE('%d/%m/%Y', data_evento_date) AS data_formatada,
            horario_evento
        FROM filtra_celulares_sem_whats
        WHERE celular_disparo IS NOT NULL AND data_evento_date IS NOT NULL
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
        endereco_evento AS endereco,
        data_formatada AS data,
        horario_evento AS horario
    FROM formatted
    WHERE celular_disparo IS NOT NULL;
END IF;
