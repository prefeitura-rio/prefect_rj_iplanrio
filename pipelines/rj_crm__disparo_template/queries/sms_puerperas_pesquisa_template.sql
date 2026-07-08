-- {pesquisa_id}
WITH segmentacao_original AS (
    SELECT
        lpad(cast(cpf as string) , 11, '0') as cpf,
        nome,
        data_alta_internacao,
        telefones_gestante
    FROM `rj-sms.projeto_whatsapp.sisare_alta_maternidade`
    -- from `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste`
    WHERE cpf is not null
        and cpf != '00000000000'
    AND CURRENT_DATE('America/Sao_Paulo') in
        (
        {datas_internacao}
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
            and sd.nome_hsm = '{nome_hsm_anterior_placeholder}'
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
            and sd.nome_hsm = '{nome_hsm_placeholder}'
            and sd.envio_datahora >= DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL {intervalo_filtro_disparados} DAY)
            and sd.data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL {intervalo_filtro_disparados} DAY)
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
    ARRAY(SELECT x FROM UNNEST([celular_disparo_2, celular_disparo_3]) AS x WHERE x IS NOT NULL AND x != celular_disparo) AS others
from final
where celular_disparo is not null
