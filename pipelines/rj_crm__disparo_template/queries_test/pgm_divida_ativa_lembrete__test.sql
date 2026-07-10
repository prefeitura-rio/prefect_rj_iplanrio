-- Teste da query queries_dev/pgm_divida_ativa_lembrete.sql (mirror de queries/pgm_divida_ativa_lembrete.sql)
-- Placeholders já substituídos com os valores reais do schedule "daily-pgm-divida-ativa-lembrete-prod-v1"
-- (scheduler_sf.yaml): nome_hsm_cobranca_placeholder -> 'pgm_divida_ativa_prod_v2',
-- nome_hsm_lembrete_placeholder -> 'pgmdividaativalembreteprodv1'.
-- Executa direto no BigQuery. Passos 1 a 3 inserem um CPF de teste que já "recebeu a
-- cobrança" e tem uma guia com cota vencendo em D+2 (exigido pelo filtro final da query),
-- caindo assim em todos os filtros; passo 4 é a query em si.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
WHERE cpf = '12pgmlembr1';

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
WHERE cpf_cnpj = '12pgmlembr1';

DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE cpf = '12pgmlembr1'
    AND nome_hsm IN ('pgm_divida_ativa_prod_v2', 'pgmdividaativalembreteprodv1');

-- ===================== 1) INSERT: pessoa física de teste (RMI) =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica`
(cpf, nome, nome_social, telefone)
VALUES
(
    '12pgmlembr1',
    'MARIA SALESFORCE LEMBRETE',
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




-- ===================== 2) INSERT: contribuinte com guia/cota vencendo em D+2 =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte`
(id_pessoa, cpf_cnpj, tipo_pessoa, nome, cdas_associadas, guias_pagamento)
VALUES
(
    999999992,
    '12pgmlembr1',
    STRUCT(1 AS tipo_pessoa, 'Pessoa Física' AS descricao_tipo_pessoa),
    'MARIA SALESFORCE LEMBRETE',
    [STRUCT(
        1 AS id_certidao_divida_ativa,
        2024 AS ano_de_inscricao_na_divida,
        DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 1 MONTH) AS data_geracao_cda,
        CAST(NULL AS DATETIME) AS data_ultima_alteracao_situacao,
        CAST(NULL AS STRING) AS numero_processo_associado,
        CAST(1000 AS NUMERIC) AS valor_original_divida_ativa,
        CAST(1000 AS NUMERIC) AS valor_saldo_devido,
        CAST(0 AS NUMERIC) AS valor_multa_moratoria,
        CAST(0 AS NUMERIC) AS valor_juros_moratorios,
        CAST(0 AS NUMERIC) AS valor_mora,
        CAST(0 AS NUMERIC) AS valor_juros_mora,
        CAST(0 AS NUMERIC) AS valor_pago_principal,
        CAST(0 AS NUMERIC) AS valor_honorarios,
        CAST(0 AS NUMERIC) AS valor_mora_smf_iptu,
        CAST(NULL AS STRUCT<codigo_receita_cda STRING, nome_receita STRING>) AS tipo_receita,
        CAST(NULL AS STRUCT<id_entidade_credora INT64, nome_entidade_credora STRING>) AS entidade_credora,
        CAST(NULL AS STRUCT<codigo_fase_cobranca INT64, nome_fase_cobranca STRING>) AS fase_cobranca,
        -- não pode ser 'Paga' nem 'Parcelamento Irregular' (filtro descricao_situacao_cda not in)
        STRUCT(1 AS id_situacao_cda, 'Inscrita' AS descricao_situacao_cda) AS situacao_cda,
        CAST(NULL AS STRUCT<id_natureza_divida INT64, nome_natureza_divida STRING>) AS natureza_divida_ativa,
        CAST(NULL AS STRING) AS dados_complementares,
        1 AS quantidade_devedores_cda,
        CAST([] AS ARRAY<STRUCT<id_pessoa INT64, cpf_cnpj STRING, tipo_pessoa INT64, descricao_tipo_pessoa STRING, nome STRING>>) AS devedores_vinculados_cda,
        FALSE AS possui_imovel_associado,
        CAST(NULL AS STRUCT<
            endereco STRUCT<codigo_logradouro INT64, nome_logradouro STRING, numero_porta STRING, complemento_endereco STRING, bairro STRING, cep STRING>,
            tipologia_imovel STRUCT<id_tipologia_imovel INT64, nome_tipologia_imovel STRING>,
            utilizacao_imovel STRUCT<id_utilizacao_imovel INT64, nome_utilizacao_imovel STRING>
        >) AS imovel_associado
    )],
    [STRUCT(
        1 AS id_guia_pagamento,
        -- gerada depois do disparo de cobrança inserido no passo 3 (hoje - 10 dias)
        DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 5 DAY) AS data_criacao_guia,
        CAST(NULL AS STRING) AS numero_processo_associado,
        CAST(NULL AS STRUCT<id_tipo_guia INT64, nome_tipo_guia STRING>) AS tipo_guia,
        CAST(NULL AS STRUCT<id_tipo_pagamento INT64, nome_tipo_pagamento STRING>) AS tipo_pagamento,
        CAST(NULL AS STRUCT<codigo_receita STRING, nome_receita STRING>) AS tipo_receita,
        CAST(NULL AS STRUCT<id_situacao_guia_pagamento INT64, nome_situacao_guia_pagamento STRING>) AS situacao,
        1 AS quantidade_cotas,
        [STRUCT(
            1 AS id_cota_guia_pagamento,
            FALSE AS cota_paga,
            FALSE AS cota_substituida,
            FALSE AS substituta_de_outra,
            CAST(1000.00 AS NUMERIC) AS valor_cota_guia_pagamento,
            CURRENT_DATETIME('America/Sao_Paulo') AS data_emissao,
            -- filtro exige data_vencimento = hoje + 2 dias
            DATE_ADD(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 2 DAY) AS data_vencimento,
            CAST(NULL AS DATETIME) AS data_pagamento,
            CAST(1000.00 AS NUMERIC) AS valor_principal,
            CAST(0.00 AS NUMERIC) AS valor_honorarios,
            CAST(0.00 AS NUMERIC) AS valor_juros,
            CAST(0.00 AS NUMERIC) AS valor_grerj,
            CAST(0.00 AS NUMERIC) AS valor_juros_principal,
            CAST(0.00 AS NUMERIC) AS valor_juros_honorarios,
            CAST(NULL AS STRING) AS observacoes,
            CAST(NULL AS NUMERIC) AS valor_principal_pago,
            CAST(NULL AS NUMERIC) AS valor_juros_pago,
            CAST(NULL AS NUMERIC) AS valor_honorarios_pago,
            CAST(NULL AS NUMERIC) AS valor_grerj_pago,
            CAST(NULL AS NUMERIC) AS valor_juros_honorarios_pago,
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
    'pgm_divida_ativa_prod_v2',
    '0000000000000000',
    '1444750391021606',
    'UTILITY',
    'WhatsApp',
    'Mensagem de teste - cobrança',
    'Parar de receber mensagem? Digite PARAR',
    '5521989190512',
    '12pgmlembr1',
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 10 DAY),
    DATETIME_SUB(CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL 10 DAY),
    NULL,
    NULL,
    NULL,
    FALSE,
    FALSE,
    'delivered',
    3,
    DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 10 DAY)
);

-- ===================== 4) QUERY (queries_dev/pgm_divida_ativa_lembrete.sql com placeholders substituídos) =====================
with
    -- Substitui a leitura de rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*
    -- (só tinha histórico da API antiga da Wetalkie) por
    -- rj-crm-registry.brutos_salesforce.status_disparo (alimentada pelos disparos via SFTP/Salesforce).
    -- Chave de busca agora é nome_hsm (STRING, == campaign_name), não mais templateId (INT64 da Wetalkie).
    disparos_divida_ativa as (
        -- quem recebeu os disparos de whatsapp sem erros
        select distinct cpf,
        contato_telefone as celular_disparo,
        DATE(envio_datahora) as data_disparo,
        nome_hsm as id_hsm
        from `rj-crm-registry.brutos_salesforce.status_disparo`
        where nome_hsm in (
            'pgm_divida_ativa_prod_v2',
            'pgmdividaativalembreteprodv1'
        )
        and indicador_falha = FALSE
    ),
    disparos as (
        -- traz último disparo de cobrança e lembrete
        select cpf, celular_disparo,
        max(case when id_hsm = 'pgm_divida_ativa_prod_v2' then data_disparo else null end) as data_disparo_cobranca,
        max(case when id_hsm = 'pgmdividaativalembreteprodv1' then data_disparo else null end) as data_disparo_lembrete,
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
