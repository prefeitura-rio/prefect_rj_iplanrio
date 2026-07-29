with
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
        nome_hsm as id_hsm
        from `rj-crm-registry.brutos_salesforce.status_disparo`
        where nome_hsm in (
            '{nome_hsm_cobranca_placeholder}',
            '{nome_hsm_lembrete_placeholder}'
        )
        and indicador_quarentena = FALSE

        UNION ALL

        -- Legado: histórico via Wetalkie antes da migração (196/239 = cobrança, 227 = lembrete)
        select distinct targetexternalid as cpf,
        flattarget as celular_disparo,
        DATE(createdate) as data_disparo,
        CASE
            WHEN templateId IN ({id_hsm_legado_cobranca_placeholder}) THEN '{nome_hsm_cobranca_placeholder}'
            WHEN templateId IN ({id_hsm_legado_lembrete_placeholder}) THEN '{nome_hsm_lembrete_placeholder}'
        END as id_hsm
        from `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
        where templateid in ({id_hsm_legado_cobranca_placeholder}, {id_hsm_legado_lembrete_placeholder})
        and faileddate is null
    ),
    disparos as (
        -- traz último disparo de cobrança e lembrete
        select cpf, celular_disparo,
        max(case when id_hsm = '{nome_hsm_cobranca_placeholder}' then data_disparo else null end) as data_disparo_cobranca,
        max(case when id_hsm = '{nome_hsm_lembrete_placeholder}' then data_disparo else null end) as data_disparo_lembrete,
        from disparos_divida_ativa
        group by 1, 2
    ),
    filtra_contribuinte as (
        select
            disparos.*,
            contribuinte.guias_pagamento,
            contribuinte.cdas_associadas,
            quantidade_cotas_devidas_vencidas
        FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte` AS contribuinte
        INNER JOIN disparos ON lpad(cpf_cnpj, 11, "0") = disparos.cpf
    ),
    divida_ativa as (
        -- seleciona dados da tabela de dívida ativa do lake
        SELECT
        filtra_contribuinte.* except(guias_pagamento, cdas_associadas),
        quantidade_cotas_devidas_vencidas,
        cotas AS cotas,
        cdas_associadas.id_certidao_divida_ativa as id_certidao_divida_ativa,
        guia_pagamento.id_guia_pagamento,
        guia_pagamento.numero_processo_associado,
        guia_pagamento.data_criacao_guia,
        cdas_associadas.situacao_cda.descricao_situacao_cda as descricao_situacao_cda,
        FROM filtra_contribuinte
        CROSS JOIN UNNEST(guias_pagamento) AS guia_pagamento
        CROSS JOIN UNNEST(guia_pagamento.cotas) AS cotas
        CROSS JOIn UNNEST(cdas_associadas) as cdas_associadas
        -- criacao da guia de pagamento após o disparo
        where data_disparo_cobranca <= guia_pagamento.data_criacao_guia or guia_pagamento.data_criacao_guia is null -- deixar para não enviar para guias abertas antes do disparo dessa pessoa
          and cotas.observacoes != "Encaminhada para Protesto"  -- TODO: nesse caso temos mais de uma cota vencendo no mesmo dia pra mesma pessoa, ignoramos essas pessoas, disparamos mais de uma HSM ou mudamos a HSM? para visualizar usar where rn1>1 na próxima query.
    ),
    remove_duplicados as (
        select distinct
        cpf,
        celular_disparo,
        cotas.codigo_barras,
        cotas.codigo_qr_pix,
        data_disparo_cobranca,
        id_guia_pagamento as guia,
        descricao_situacao_cda,
        cotas.data_vencimento as data_vencimento,
        cotas.valor_cota_guia_pagamento as valor_cota,
        -- seleciono a última guia gerada por pessoa para cada cda
        row_number() over (partition by cpf, id_certidao_divida_ativa order by data_criacao_guia desc, cotas.data_vencimento) as rn, --pego a última guia gerada por pessoa para cada cda e dentro dessa a com menor data de vencimento que não foi paga, com isso já elimino quem está com guias atrasadas não pagas
        -- rn1 > 1 significa que a pessoa tem mais de uma cota vencendo no mesmo dia com valores distintos,
        -- aparentemente acontece quando cotas.observacoes != "Encaminhada para Protesto"
        rank() over (partition by cpf, cotas.data_vencimento order by cotas.valor_cota_guia_pagamento desc) as rn1
        from divida_ativa
        where cotas.cota_paga = false -- esse filtro precisa estar aqui por conta do rn e por filtrarmos cotas_pagas depois de pegar a última guia de pagamento
        -- and quantidade_cotas_devidas_vencidas = 0 -- nao lembrar quem já está com cota atrasada pq ela deveria gerar outra guia, está comentado pois em alguns casos parece estar errado
        and descricao_situacao_cda not in ("Paga", "Parcelamento Irregular") -- não mexer na localização desse filtro
        and cotas.codigo_barras is not null
        and cotas.codigo_qr_pix is not null

    ),
    remove_optout as (
        select remove_duplicados.*,
        COALESCE(if(pf.nome_social = "", null, pf.nome_social), pf.nome) AS nome,
        from remove_duplicados
        left join `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica` pf using(cpf)
        where pf.telefone.principal.indicador_optout = false
        and pf.telefone.principal.indicador_quarentena = false
    )

    -- Tabela simples (sem TO_JSON_STRING). 'externalId' é controle interno (dedup por
    -- CPF) e é descartado do CSV pelo de_columns antes do envio à Data Extension.
    select
      distinct --distinct de novo porque temos id_certidao_divida_ativa com mesmo id_guia_pagamento
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
        ) AS nome_sobrenome,
        data_vencimento,
        guia AS numero_guia,
        CONCAT('R$ ', REPLACE(cast(valor_cota as string), '.', ',')) AS valor_guia,
        codigo_barras AS cod_boleto,
        codigo_qr_pix AS cod_pix
    from remove_optout
    where rn=1
    and data_vencimento = date_add(current_date("America/Sao_Paulo"), interval 2 day) -- esse filtro tem que estar aqui em vez de dentro do remove_duplicados pq lá selecionamos a com menor data de vencimento que não foi paga, com isso já elimino quem está com guias atrasadas não pagas
