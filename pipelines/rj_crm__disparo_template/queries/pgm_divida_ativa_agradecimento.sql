WITH
-- Combina o histórico novo (status_disparo, alimentado via SFTP/Salesforce) com o
-- legado da Wetalkie (fluxo_atendimento_*, por templateId numérico), enquanto a
-- status_disparo não acumula todo o histórico pré-migração. O templateId legado é
-- normalizado pro mesmo rótulo (nome_hsm) usado pelos placeholders atuais, então a
-- agregação abaixo não precisa saber de qual fonte cada disparo veio.
disparos_divida_ativa as (
    -- quem recebeu os disparos de whatsapp sem erros (via Salesforce/SFTP)
    select distinct cpf,
    contato_telefone as celular_disparo,
    DATE(envio_datahora) as data_disparo,
    nome_hsm as id_hsm,
    falha_datahora as failedDate
    from `rj-crm-registry.brutos_salesforce.status_disparo`
    where nome_hsm in (
        '{nome_hsm_cobranca_placeholder}',
        '{nome_hsm_lembrete_placeholder}',
        '{nome_hsm_agradecimento_placeholder}'
    )

    UNION ALL

    -- Legado: histórico via Wetalkie antes da migração
    -- (196/239 = cobrança, 227 = lembrete, 231 = agradecimento)
    select distinct targetexternalid as cpf,
    flattarget as celular_disparo,
    DATE(createdate) as data_disparo,
    CASE
        WHEN templateId IN ({id_hsm_legado_cobranca_placeholder}) THEN '{nome_hsm_cobranca_placeholder}'
        WHEN templateId IN ({id_hsm_legado_lembrete_placeholder}) THEN '{nome_hsm_lembrete_placeholder}'
        WHEN templateId IN ({id_hsm_legado_agradecimento_placeholder}) THEN '{nome_hsm_agradecimento_placeholder}'
    END as id_hsm,
    failedDate
    from `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
    where templateid in ({id_hsm_legado_cobranca_placeholder}, {id_hsm_legado_lembrete_placeholder}, {id_hsm_legado_agradecimento_placeholder})
),

disparos as (
    -- traz último disparo de cobrança, lembrete e agradecimento
    select cpf, celular_disparo,
    max(failedDate) as ultima_data_falha,
    max(case when id_hsm = '{nome_hsm_cobranca_placeholder}' then data_disparo else null end) as data_disparo_cobranca,
    max(case when id_hsm = '{nome_hsm_lembrete_placeholder}' then data_disparo else null end) as data_disparo_lembrete,
    max(case when id_hsm = '{nome_hsm_agradecimento_placeholder}' then data_disparo else null end) as data_disparo_agradecimento,
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
    FROM `rj-iplanrio.divida_ativa.contribuinte` AS contribuinte
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
            AND COALESCE(c.observacoes, "") != "Encaminhada para Protesto"
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
    left join `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf using(cpf)
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
