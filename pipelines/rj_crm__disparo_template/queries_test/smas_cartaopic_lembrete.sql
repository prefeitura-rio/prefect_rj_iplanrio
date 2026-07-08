-- Cartão PIC - Lembrete (smascartaoprimeirainfancialembreteprodv4)
-- Query de teste: lê de rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste
-- DE esperada: whatsapp_smas_cartaopic_lembrete_<timestamp>
-- Colunas da DE: nome_sobrenome, endereco, data, horario, telefone, SubscriberKey, LOCALE

CREATE OR REPLACE TABLE `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
  NUM_CPF_RESPONSAVEL STRING,
  NOME_RESPONSAVEL STRING,
  NUM_TEL_CONTATO_1_FAM STRING,
  NUM_TEL_CONTATO_2_FAM STRING,
  LOCAL_ENTREGA_PREVISTO STRING,
  ENDERECO_ENTREGA_PREVISTO STRING,
  DATA_ENTREGA_PREVISTA DATE,
  HORA_ENTREGA_PREVISTA STRING,
  CRAS STRING,
  ENDERECO_CRAS STRING,
  DATA_RETIRADA_CRAS DATE,
  HORA_RETIRADA_CRAS STRING,
  ENVELOPE STRING,
  NUM_TEL_CONTATO_1_DECLARADO STRING,
  NOME_SOCIAL_RESPONSAVEL STRING,
  DATA_ENTREGA DATE,
  TIPO_ENTREGA STRING,
  LOCAL_ENTREGA STRING,
  RESPONSAVEL_PELA_RETIRADA STRING,
  STATUS STRING,
  FLAG_ENTREGA BOOLEAN
);

INSERT INTO `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
  NUM_CPF_RESPONSAVEL,
  NOME_RESPONSAVEL,
  NUM_TEL_CONTATO_1_FAM,
  NUM_TEL_CONTATO_2_FAM,
  LOCAL_ENTREGA_PREVISTO,
  ENDERECO_ENTREGA_PREVISTO,
  DATA_ENTREGA_PREVISTA,
  HORA_ENTREGA_PREVISTA,
  CRAS,
  ENDERECO_CRAS,
  DATA_RETIRADA_CRAS,
  HORA_RETIRADA_CRAS,
  ENVELOPE,
  NUM_TEL_CONTATO_1_DECLARADO,
  NOME_SOCIAL_RESPONSAVEL,
  DATA_ENTREGA,
  TIPO_ENTREGA,
  LOCAL_ENTREGA,
  RESPONSAVEL_PELA_RETIRADA,
  STATUS,
  FLAG_ENTREGA
)
VALUES (
  '00000000001',
  'patricia salesforce',
  '5511984677798',
  NULL,
  'Cras Vila Isabel',
  'Rua Torres Homem, 120 - Vila Isabel',
  DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY),
  '10:00',
  'CRAS Vila Isabel',
  'Rua Torres Homem, 120 - Vila Isabel',
  DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY),
  '10:15',
  'E001',
  '5511984677798',
  NULL,
  DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY),
  'retirada no CRAS',
  'CRAS Vila Isabel',
  'PATRICIA SALESFORCE',
  'aguardando evento',
  TRUE
);

WITH dados_pic AS (
    SELECT
        LPAD(CAST(NUM_CPF_RESPONSAVEL AS STRING), 11, '0') AS cpf,
        TRIM(COALESCE(NOME_SOCIAL_RESPONSAVEL, NOME_RESPONSAVEL)) AS nome_completo,
        TRIM(
            CONCAT(
                COALESCE(CRAS, LOCAL_ENTREGA_PREVISTO),
                IF(
                    COALESCE(ENDERECO_CRAS, ENDERECO_ENTREGA_PREVISTO) IS NOT NULL
                    AND COALESCE(ENDERECO_CRAS, ENDERECO_ENTREGA_PREVISTO) != '',
                    CONCAT(' (', COALESCE(ENDERECO_CRAS, ENDERECO_ENTREGA_PREVISTO), ')'),
                    ''
                )
            )
        ) AS endereco_evento,
        COALESCE(DATA_RETIRADA_CRAS, DATA_ENTREGA_PREVISTA) AS data_evento,
        COALESCE(HORA_RETIRADA_CRAS, HORA_ENTREGA_PREVISTA) AS horario_evento,
        NUM_TEL_CONTATO_1_FAM AS celular_1,
        NUM_TEL_CONTATO_2_FAM AS celular_2,
    FROM `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste`
    WHERE STATUS != 'não retirado'
        AND FLAG_ENTREGA = TRUE
),
formatted AS (
    SELECT
        COALESCE(celular_1, celular_2) AS telefone,
        cpf,
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
        endereco_evento AS endereco,
        FORMAT_DATE('%d/%m/%Y', data_evento) AS data,
        horario_evento AS horario,
    FROM dados_pic
    WHERE COALESCE(celular_1, celular_2) IS NOT NULL
)
SELECT
    telefone,
    cpf AS SubscriberKey,
    nome_sobrenome,
    endereco,
    data,
    horario
FROM formatted
