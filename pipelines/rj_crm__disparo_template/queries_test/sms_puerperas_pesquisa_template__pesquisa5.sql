-- Pesquisa 5
WITH segmentacao_original AS (
    SELECT
        lpad(cast(cpf as string) , 11, '0') as cpf,
        nome,
        data_alta_internacao,
        telefones_gestante
    FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`
    -- from `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste`
    WHERE cpf is not null
        and cpf != '00000000000'
    AND CURRENT_DATE('America/Sao_Paulo') in
        (
        DATE_ADD(DATE(data_alta_internacao), INTERVAL 25 DAY),
DATE_ADD(DATE(data_alta_internacao), INTERVAL 28 DAY)

        )
        and nome_maternidade_alta like '%MARIA AMELIA%'
),
telefones as (
select
    lpad(cast(cpf as string) , 11, '0') as cpf,
    nome,
    MAX(data_alta_internacao) as data_alta_internacao,
    MAX(IF(telefone.prioridade = '1', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_1,
    MAX(IF(telefone.prioridade = '2', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_2,
    MAX(IF(telefone.prioridade = '3', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_3
FROM segmentacao_original,
UNNEST(telefones_gestante) AS telefone
group by cpf, nome
),
dados_rmi as (
-- os dados do rmi serão usados apenas caso a puérpera não tenha nenhum telefone atribuido
        SELECT
            cpf,
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_PHONE`(CONCAT(ifnull(telefone.principal.ddi, '55'), telefone.principal.ddd, telefone.principal.valor)) AS celular_disparo_rmi,
        FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi
        WHERE menor_idade is False
        and obito.indicador is False
        and telefone.principal.estrategia_envio in ('ENVIAR', 'TESTAR')
),
enriquece_rmi as (
    select
        telefones.*,
        dados_rmi.celular_disparo_rmi
    from telefones
    left join dados_rmi using(cpf)
),
-- Substitui a leitura de rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*
-- (só tinha histórico da API antiga da Wetalkie) por
-- rj-crm-registry.brutos_salesforce.status_disparo (alimentada pelos disparos via SFTP/Salesforce).
-- Chave de busca agora é nome_hsm (STRING, == campaign_name), não mais templateId (INT64 da Wetalkie).
status_final_telefone AS (
    -- verifica se telefones já tiveram falha, passo necessário já que o telefone principal não vem do RMI. Caso contrário, seria só necessário filtrar pela estratégia de envio
    -- qualquer falha conta (não só o código 131026 de "número sem WhatsApp")
    SELECT
        contato_telefone AS flatTarget,
        indicador_falha AS falhou
    FROM `rj-crm-registry.brutos_salesforce.status_disparo`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contato_telefone
        ORDER BY envio_datahora DESC
    ) = 1
),
filtra_falhas AS (
    SELECT
        distinct
        s.* EXCEPT(celular_disparo_1, celular_disparo_2, celular_disparo_3, celular_disparo_rmi),

        IF(f1.falhou, NULL, s.celular_disparo_1) AS celular_disparo_1,
        IF(f2.falhou, NULL, s.celular_disparo_2) AS celular_disparo_2,
        IF(f3.falhou, NULL, s.celular_disparo_3) AS celular_disparo_3,
        IF(f4.falhou, NULL, s.celular_disparo_rmi) AS celular_disparo_rmi,

        COALESCE(
        IF(f1.falhou, NULL, s.celular_disparo_1),
        IF(f2.falhou, NULL, s.celular_disparo_2),
        IF(f3.falhou, NULL, s.celular_disparo_3),
        IF(f4.falhou, NULL, s.celular_disparo_rmi)
        ) AS celular_disparo

    FROM enriquece_rmi s
    LEFT JOIN status_final_telefone f1
        ON f1.flatTarget = s.celular_disparo_1

    LEFT JOIN status_final_telefone f2
        ON f2.flatTarget = s.celular_disparo_2

    LEFT JOIN status_final_telefone f3
        ON f3.flatTarget = s.celular_disparo_3

    LEFT JOIN status_final_telefone f4
        ON f4.flatTarget = s.celular_disparo_rmi
),
filtra_recebeu_primeira_hsm as (
    -- verifica se esse cpf recebeu a primeira mensagem (D0, nome_hsm_anterior_placeholder) nos últimos 50 dias
    select distinct filtra_falhas.*
    FROM filtra_falhas
    left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
            on sd.cpf = filtra_falhas.cpf
            and sd.nome_hsm = 'smspuerperasdisparo25'
            and sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 50 DAY)
            and sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 51 DAY)
        where sd.cpf is not null
),
filtra_disparados as (
    -- verifica se esse cpf já recebeu essa mesma mensagem (nome_hsm_placeholder) nos últimos x dias
    select filtra_recebeu_primeira_hsm.*
    FROM filtra_recebeu_primeira_hsm
    left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
            on sd.cpf = filtra_recebeu_primeira_hsm.cpf
            and sd.nome_hsm = 'smspuerperasdisparo12'
            and sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY)
            and sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            and sd.indicador_falha = FALSE
        where sd.cpf is null
),
final as (
    select filtra_disparados.*
    from filtra_disparados
)
-- Tabela simples (sem TO_JSON_STRING). 'externalId' (dedup por CPF) e 'others'
-- (retentativas) são controle interno; o de_columns de cada schedule decide quais
-- colunas (ex.: nome) realmente vão pra Data Extension.
SELECT
    celular_disparo AS telefone,
    CAST(cpf AS STRING) AS SubscriberKey,
    CAST(cpf AS STRING) AS externalId,
    INITCAP(
        IF(
            ARRAY_LENGTH(SPLIT(nome, ' ')) > 1,
            CONCAT(SPLIT(nome, ' ')[SAFE_OFFSET(0)], ' '),
            nome
        )
    ) AS nome,
    CAST(DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), DATE(data_alta_internacao), DAY) AS STRING) AS numero_dias,
    ARRAY(SELECT x FROM UNNEST([celular_disparo_2, celular_disparo_3]) AS x WHERE x IS NOT NULL AND x != celular_disparo) AS others
from final
where celular_disparo is not null

DELETE from `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`
where nome = 'MARIA SALESFORCE'

INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`
(
    cpf,
    nome,
    data_alta_internacao,
    cnes_maternidade_alta,
    nome_maternidade_alta,
    data_parto,
    id_desfecho_gestacao,
    desfecho_gestacao,
    telefones_gestante
)
VALUES
(
    '12pesquisa5',
    'MARIA SALESFORCE',
    -- pesquisa 5 dispara em D+25 ou D+28
    DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 25 DAY),
    '1234567',
    'MATERNIDADE MARIA AMELIA',
    CURRENT_DATE('America/Sao_Paulo'),
    1,
    'RN Nascido Vivo',
    [STRUCT(
        '5521989190512' AS telefone_original,
        'TESTE' AS origem,
        '1' AS prioridade,
        '5521989190512' AS telefone_valido_whatsapp,
        CAST(NULL AS STRING) AS motivo_invalidacao_telefone
    )]
);

-- Simula que o CPF já recebeu a primeira mensagem (D0, smspuerperasdisparo25),
-- pré-requisito do CTE filtra_recebeu_primeira_hsm para esta pesquisa disparar.
DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12345678901'
    AND nome_hsm = 'smspuerperasdisparo25'

INSERT INTO `rj-crm-registry.brutos_salesforce.status_disparo`
(
    id_jornada,
    nome_jornada,
    chave_atividade,
    nome_atividade,
    id_ativo,
    nome_hsm,
    id_hsm,
    id_waba,
    categoria_hsm,
    canal,
    texto_hsm,
    rodape_hsm,
    status_ativo,
    ativo_criacao_datahora,
    ativo_modificacao_datahora,
    contato_telefone,
    cpf,
    envio_datahora,
    entrega_datahora,
    leitura_datahora,
    falha_datahora,
    descricao_falha,
    indicador_falha,
    indicador_quarentena,
    status_disparo,
    id_status_disparo,
    data_particao
)
VALUES
(
    GENERATE_UUID(),
    'SMS_PUERPERAS_D0_EXPLICA_PESQUISA',
    'WHATSAPPACTIVITY-1',
    'puerpera_d0_explica_pesquisa',
    100000,
    'smspuerperasdisparo25',
    '0000000000000000',
    '1444750391021606',
    'UTILITY',
    'WhatsApp',
    'Mensagem de teste - primeira HSM (D0)',
    'Parar de receber mensagem? Digite PARAR',
    'Complete',
    CAST(DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 DAY) AS STRING),
    CAST(DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 2 DAY) AS STRING),
    '5521989190512',
    '12pesquisa5',
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY),
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY),
    NULL,
    NULL,
    NULL,
    FALSE,
    FALSE,
    'delivered',
    3,
    DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
);
