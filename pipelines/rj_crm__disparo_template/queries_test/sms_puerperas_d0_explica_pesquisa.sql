-- Teste da query queries_dev/sms_puerperas_d0_explica_pesquisa.sql (mirror de queries/sms_puerperas_d0_explica_pesquisa.sql)
-- Executa direto no BigQuery. Passo 1 limpa dados de teste anteriores; passo 2 insere uma
-- alta de maternidade MARIA AMELIA de D-1, sem nenhum disparo anterior para esse CPF; passo 3
-- é a query em si.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE from `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`
where nome = 'MARIA SALESFORCE';

-- limpa histórico de disparo do telefone de teste em produção: sem isso, uma falha
-- registrada em qualquer teste anterior (de qualquer campanha) faz status_final_telefone
-- anular esse número pra sempre e a query nunca retorna linha pra ele
DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE contato_telefone = '5521989190512';

-- ===================== 1) INSERT: alta de maternidade (segmentação) =====================
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
    '12345678901',
    'MARIA SALESFORCE',
    DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY),
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

-- ===================== 2) QUERY (query original sms_puerperas_d0_explica_pesquisa) =====================
WITH segmentacao_original AS (
    SELECT
        lpad(cast(cpf as string) , 11, '0') as cpf,
        nome,
        data_alta_internacao,
        telefones_gestante,
    FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.sisare_alta_maternidade`
    -- from `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste`
    WHERE cpf is not null
        and cpf != '00000000000'
    AND DATE(data_alta_internacao) between
            DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY) and CURRENT_DATE('America/Sao_Paulo')
            and nome_maternidade_alta like '%MARIA AMELIA%' -- todo: remover comentario
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

    filtra_disparados as (
    -- verifica se esse cpf já recebeu essa mesma mensagem (smspuerperasdisparo25) nos últimos x dias,
    -- tanto via status_disparo quanto via wetalkie (histórico legado pré-migração;
    -- templateId 610 = HSM D0 explica pesquisa). Sem seed na wetalkie aqui de
    -- propósito: o LEFT JOIN não encontra nada e fica inerte.
        select filtra_falhas.*
        FROM filtra_falhas
        left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
                on sd.cpf = filtra_falhas.cpf
                and sd.nome_hsm = 'smspuerperasdisparo25'
                and sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 300 DAY) -- pessoa só recebe essa mensagem cerca de uma vez por ano podendo pegar mais de uma gravidez
                and sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 300 DAY)
                and sd.indicador_falha = FALSE
        left join `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
                on fl.targetexternalid = filtra_falhas.cpf
                and fl.templateId = 610
                and fl.createDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 300 DAY)
                and fl.status in ('PROCESSING', 'SENT')
            where sd.cpf is null and fl.flattarget is null
    ),
    final as (
        select filtra_disparados.* from filtra_disparados
    )
    -- Tabela simples (sem TO_JSON_STRING). 'externalId' (dedup por CPF) e 'others'
    -- (retentativas) são controle interno, descartados do CSV pelo de_columns.
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
        ARRAY(SELECT x FROM UNNEST([celular_disparo_2, celular_disparo_3]) AS x WHERE x IS NOT NULL AND x != celular_disparo) AS others
    from final
    where celular_disparo is not null
