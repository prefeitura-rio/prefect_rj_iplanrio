WITH segmentacao_original AS (
    SELECT
        lpad(cast(cpf as string) , 11, '0') as cpf,
        nome,
        nome_maternidade_agendada AS maternidade,
        data_hora_agendamento_visita_maternidade,

        MAX(IF(telefone.prioridade = '1', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_1,
        MAX(IF(telefone.prioridade = '2', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_2,
        MAX(IF(telefone.prioridade = '3', telefone.telefone_valido_whatsapp, NULL)) AS celular_disparo_3

    FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.siscegonha_agendamento_maternidade`,
    -- FROM `rj-crm-registry-dev.brutos_sms.siscegonha_agendamento_maternidade_teste`,
    UNNEST(telefones_gestante) AS telefone

    WHERE cpf is not null
        and cpf != '00000000000'
        AND time(data_hora_agendamento_visita_maternidade) != '00:00:00'
        AND DATE(data_hora_criacao_agendamento) =
            DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY)
            and nome_maternidade_agendada like '%MARIA AMELIA%'
    GROUP BY
        cpf,
        nome,
        maternidade,
        data_hora_agendamento_visita_maternidade
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
    enriquece_rmi as 
    (
    select
    segmentacao_original.*,
    dados_rmi.celular_disparo_rmi
    from segmentacao_original
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
    -- verifica se esse cpf já recebeu essa mesma mensagem (_sms_puerpera_disp1_gestantev2) nos últimos x dias
        select filtra_falhas.*
        FROM filtra_falhas
        left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
                on sd.cpf = filtra_falhas.cpf
                and sd.nome_hsm = '_sms_puerpera_disp1_gestantev2'
                and sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY)
                and sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                and sd.indicador_falha = FALSE
            where sd.cpf is null
    ),
    final as (
        select
        filtra_disparados.*,
        from filtra_disparados
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
        maternidade,
        date(data_hora_agendamento_visita_maternidade) as data,
        time(data_hora_agendamento_visita_maternidade) as hora,
        ARRAY(SELECT x FROM UNNEST([celular_disparo_2, celular_disparo_3]) AS x WHERE x IS NOT NULL AND x != celular_disparo) AS others
    from final
    where celular_disparo is not null

DELETE from `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.siscegonha_agendamento_maternidade`
where nome = 'MARIA SALESFORCE'

INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__brutos_sms.siscegonha_agendamento_maternidade`
(
    id_agendamento_gestante,
    nome,
    cpf,
    cnes_maternidade_agendada,
    nome_maternidade_agendada,
    data_hora_criacao_agendamento,
    data_hora_agendamento_visita_maternidade,
    telefones_gestante,
    nome_acompanhante,
    telefone_acompanhante
)
VALUES
(
    'TESTE-001',
    'MARIA SALESFORCE',
    '12345678901',
    '1234567',
    'MATERNIDADE MARIA AMELIA',
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 DAY),
    DATETIME(DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 DAY), TIME '21:00:00'),
    [STRUCT(
        '5521989190512' AS telefone_original,
        'TESTE' AS origem,
        '1' AS prioridade,
        '5521989190512' AS telefone_valido_whatsapp,
        CAST(NULL AS STRING) AS motivo_invalidacao_telefone
    )],
    CAST(NULL AS STRING),
    CAST(NULL AS STRUCT<telefone_original STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>)
);