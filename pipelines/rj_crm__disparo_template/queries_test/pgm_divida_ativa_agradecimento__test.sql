-- Teste da query queries_dev/pgm_divida_ativa_agradecimento.sql (mirror de queries/pgm_divida_ativa_agradecimento.sql)
-- Placeholders já substituídos com os valores reais do schedule "daily-pgm-divida-ativa-confirma-pgto-prod-v1"
-- (scheduler_sf.yaml): nome_hsm_cobranca_placeholder -> 'pgm-divida-ativa-prod-v2',
-- nome_hsm_lembrete_placeholder -> 'pgm-divida-ativa-lembrete-prod-v1',
-- nome_hsm_agradecimento_placeholder -> 'pgm-divida-ativa-confirma-pgto-prod-v1'.
-- Executa direto no BigQuery. Passos 1 a 3 inserem um CPF de teste que já "recebeu a
-- cobrança" e tem uma cota paga nos últimos 5 dias (exigido pelo filtro final da query),
-- sem nenhum agradecimento anterior, caindo assim em todos os filtros; passo 4 é a query em si.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
WHERE cpf = '12pgmagrad1';

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
WHERE cpf_cnpj = '12pgmagrad1';

DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12pgmagrad1'
    AND nome_hsm IN (
        'pgm-divida-ativa-prod-v2',
        'pgm-divida-ativa-lembrete-prod-v1',
        'pgm-divida-ativa-confirma-pgto-prod-v1'
    );

-- ===================== 1) INSERT: pessoa física de teste (RMI) =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
(cpf, nome, nome_social, telefone)
VALUES
(
    '12pgmagrad1',
    'MARIA SALESFORCE AGRADECIMENTO',
    NULL,
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

-- ===================== 2) INSERT: contribuinte com cota paga nos últimos 5 dias =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
(id_pessoa, cpf_cnpj, tipo_pessoa, nome, guias_pagamento)
VALUES
(
    999999993,
    '12pgmagrad1',
    STRUCT(1 AS tipo_pessoa, 'Pessoa Física' AS descricao_tipo_pessoa),
    'MARIA SALESFORCE AGRADECIMENTO',
    [STRUCT(
        1 AS id_guia_pagamento,
        -- gerada depois do disparo de cobrança inserido no passo 3 (hoje - 15 dias)
        DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 10 DAY) AS data_criacao_guia,
        CAST(NULL AS STRING) AS numero_processo_associado,
        CAST(NULL AS STRUCT<id_tipo_guia INT64, nome_tipo_guia STRING>) AS tipo_guia,
        CAST(NULL AS STRUCT<id_tipo_pagamento INT64, nome_tipo_pagamento STRING>) AS tipo_pagamento,
        CAST(NULL AS STRUCT<codigo_receita STRING, nome_receita STRING>) AS tipo_receita,
        CAST(NULL AS STRUCT<id_situacao_guia_pagamento INT64, nome_situacao_guia_pagamento STRING>) AS situacao,
        1 AS quantidade_cotas,
        [STRUCT(
            1 AS id_cota_guia_pagamento,
            -- filtro exige cota_paga = TRUE
            TRUE AS cota_paga,
            FALSE AS cota_substituida,
            FALSE AS substituta_de_outra,
            CAST(1000.00 AS NUMERIC) AS valor_cota_guia_pagamento,
            DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 10 DAY) AS data_emissao,
            DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 3 DAY) AS data_vencimento,
            -- filtro exige data_pagamento >= hoje - 5 dias
            DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 DAY) AS data_pagamento,
            CAST(1000.00 AS NUMERIC) AS valor_principal,
            CAST(0.00 AS NUMERIC) AS valor_honorarios,
            CAST(0.00 AS NUMERIC) AS valor_juros,
            CAST(0.00 AS NUMERIC) AS valor_grerj,
            CAST(0.00 AS NUMERIC) AS valor_juros_principal,
            CAST(0.00 AS NUMERIC) AS valor_juros_honorarios,
            -- filtro exige observacoes != "Encaminhada para Protesto"
            CAST(NULL AS STRING) AS observacoes,
            CAST(1000.00 AS NUMERIC) AS valor_principal_pago,
            CAST(0.00 AS NUMERIC) AS valor_juros_pago,
            CAST(0.00 AS NUMERIC) AS valor_honorarios_pago,
            CAST(0.00 AS NUMERIC) AS valor_grerj_pago,
            CAST(0.00 AS NUMERIC) AS valor_juros_honorarios_pago,
            CAST(NULL AS INT64) AS ano_ipcae,
            '00000000000000000000000000000000000000000000' AS codigo_barras,
            CAST(NULL AS STRING) AS id_pix,
            'PIXQRCODETESTE' AS codigo_qr_pix
        )] AS cotas
    )]
);

-- ===================== 3) INSERT: simula que o CPF já recebeu a cobrança =====================
INSERT INTO `rj-crm-registry.brutos_salesforce.status_disparo`
(
    id_jornada, nome_jornada, chave_atividade, nome_atividade, id_ativo,
    nome_hsm, id_hsm, id_waba, categoria_hsm, canal, texto_hsm, rodape_hsm,
    status_ativo, ativo_criacao_datahora, ativo_modificacao_datahora,
    contato_telefone, cpf, envio_datahora, entrega_datahora, leitura_datahora,
    falha_datahora, descricao_falha, indicador_falha, indicador_quarentena,
    status_disparo, id_status_disparo, data_particao
)
VALUES
(
    GENERATE_UUID(),
    'PGM_DIVIDA_ATIVA',
    'WHATSAPPACTIVITY-1',
    'pgm_divida_ativa_cobranca',
    100000,
    'pgm-divida-ativa-prod-v2',
    '0000000000000000',
    '1444750391021606',
    'UTILITY',
    'WhatsApp',
    'Mensagem de teste - cobrança',
    'Parar de receber mensagem? Digite PARAR',
    'Complete',
    CAST(DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 15 DAY) AS STRING),
    CAST(DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 15 DAY) AS STRING),
    '5521989190512',
    '12pgmagrad1',
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 15 DAY),
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 15 DAY),
    NULL,
    NULL,
    NULL,
    FALSE,
    FALSE,
    'delivered',
    3,
    DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 15 DAY)
);

-- ===================== 4) QUERY (queries_dev/pgm_divida_ativa_agradecimento.sql com placeholders substituídos) =====================
WITH
disparos_divida_ativa as (
    -- quem recebeu os disparos de whatsapp sem erros
    select distinct cpf,
    contato_telefone as celular_disparo,
    DATE(envio_datahora) as data_disparo,
    nome_hsm as id_hsm,
    falha_datahora as failedDate
    from `rj-crm-registry.brutos_salesforce.status_disparo`
    where nome_hsm in (
        'pgm-divida-ativa-prod-v2',
        'pgm-divida-ativa-lembrete-prod-v1',
        'pgm-divida-ativa-confirma-pgto-prod-v1'
    )
),

disparos as (
    -- traz último disparo de cobrança, lembrete e agradecimento
    select cpf, celular_disparo,
    max(failedDate) as ultima_data_falha,
    max(case when id_hsm = 'pgm-divida-ativa-prod-v2' then data_disparo else null end) as data_disparo_cobranca,
    max(case when id_hsm = 'pgm-divida-ativa-lembrete-prod-v1' then data_disparo else null end) as data_disparo_lembrete,
    max(case when id_hsm = 'pgm-divida-ativa-confirma-pgto-prod-v1' then data_disparo else null end) as data_disparo_agradecimento,
    from disparos_divida_ativa
    group by 1, 2
),

disparos_filtro as (
select
*
from disparos
where
    -- filtro falhas aqui pq se falhou na segunda/última hsm ele vai continuar enviando o agradecimento e pagando para falhar
    ultima_data_falha is null
-- filtra se a pessoa já recebeu o agradecimento há pouco tempo já que ela pode ter mais de uma cota por mês
and  (data_disparo_agradecimento is null or data_disparo_agradecimento <= date_sub(current_date(), INTERVAL 180 day)) --mudar !!

),

filtra_contribuinte as (
    select
        disparos_filtro.*,
        contribuinte.guias_pagamento
    FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte` AS contribuinte
    INNER JOIN disparos_filtro ON lpad(cpf_cnpj, 11, "0") = disparos_filtro.cpf
),

divida_ativa as (
    select
    filtra_contribuinte.* except(guias_pagamento),
    guia_pagamento.data_criacao_guia,
    guia_pagamento.id_guia_pagamento,
    cotas.id_cota_guia_pagamento,
    cotas.data_pagamento,
    cotas.cota_paga

    FROM filtra_contribuinte

    INNER JOIN UNNEST(guias_pagamento) AS guia_pagamento

    -- filtra cotas antes de explodir tudo (melhora performance e corrige o problema do filtro)
    INNER JOIN UNNEST(
        ARRAY(
            SELECT c
            FROM UNNEST(guia_pagamento.cotas) c
            WHERE
            c.cota_paga = TRUE
            AND c.data_pagamento >= date_sub(current_date("America/Sao_Paulo"), interval 5 day)
            AND c.observacoes != "Encaminhada para Protesto"
        )
    ) AS cotas

    -- criacao da guia de pagamento após o disparo
    where
    (
        data_disparo_cobranca <= guia_pagamento.data_criacao_guia
        or guia_pagamento.data_criacao_guia is null
    ) -- deixar para não enviar para guias abertas antes do disparo dessa pessoa
),

remove_duplicados as (
    select
    cpf,
    celular_disparo,
    row_number() over (partition by cpf order by data_pagamento desc) as rn,
    from divida_ativa
),

remove_optout as (
    select remove_duplicados.*,
    coalesce(pf.nome_social, pf.nome) as nome,
    from remove_duplicados
    left join `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica` pf using(cpf)
    where pf.telefone.principal.indicador_optout = false
    and pf.telefone.principal.indicador_quarentena = false
)

-- Tabela simples (sem TO_JSON_STRING). 'externalId' é controle interno (dedup por
-- CPF) e é descartado do CSV pelo de_columns antes do envio à Data Extension.
select
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
from remove_optout
where rn=1
