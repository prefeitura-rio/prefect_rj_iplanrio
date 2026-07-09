-- =============================================================
-- Mirror dev de rj-iplanrio.divida_ativa.contribuinte e
-- rj-crm-registry.rmi_dados_mestres.pessoa_fisica, usados pelas
-- queries de disparo de dívida ativa (queries/pgm_divida_ativa_*.sql).
-- Convenção de nome de dataset dev: dev__dev_fantasma__<dataset_original>,
-- igual ao usado para brutos_sms (queries_dev/sms_puerperas_*.sql).
-- =============================================================

-- ===================== 1) DATASETS =====================
CREATE SCHEMA IF NOT EXISTS `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa`
OPTIONS (
    -- ajuste a location se divergir da dos outros datasets dev__dev_fantasma__*
    location = 'US'
);

CREATE SCHEMA IF NOT EXISTS `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres`
OPTIONS (
    location = 'US'
);

-- ===================== 2) rj-iplanrio.divida_ativa.contribuinte =====================
CREATE OR REPLACE TABLE `rj-crm-registry-dev.dev__dev_fantasma__divida_ativa.contribuinte` (
    id_pessoa INT64,
    cpf_cnpj STRING,
    tipo_pessoa STRUCT<
        tipo_pessoa INT64,
        descricao_tipo_pessoa STRING
    >,
    nome STRING,
    quantidade_cdas INT64,
    saldo_devido_cdas NUMERIC,
    cdas_associadas ARRAY<STRUCT<
        id_certidao_divida_ativa INT64,
        ano_de_inscricao_na_divida INT64,
        data_geracao_cda DATETIME,
        data_ultima_alteracao_situacao DATETIME,
        numero_processo_associado STRING,
        valor_original_divida_ativa NUMERIC,
        valor_saldo_devido NUMERIC,
        valor_multa_moratoria NUMERIC,
        valor_juros_moratorios NUMERIC,
        valor_mora NUMERIC,
        valor_juros_mora NUMERIC,
        valor_pago_principal NUMERIC,
        valor_honorarios NUMERIC,
        valor_mora_smf_iptu NUMERIC,
        tipo_receita STRUCT<
            codigo_receita_cda STRING,
            nome_receita STRING
        >,
        entidade_credora STRUCT<
            id_entidade_credora INT64,
            nome_entidade_credora STRING
        >,
        fase_cobranca STRUCT<
            codigo_fase_cobranca INT64,
            nome_fase_cobranca STRING
        >,
        situacao_cda STRUCT<
            id_situacao_cda INT64,
            descricao_situacao_cda STRING
        >,
        natureza_divida_ativa STRUCT<
            id_natureza_divida INT64,
            nome_natureza_divida STRING
        >,
        dados_complementares STRING,
        quantidade_devedores_cda INT64,
        devedores_vinculados_cda ARRAY<STRUCT<
            id_pessoa INT64,
            cpf_cnpj STRING,
            tipo_pessoa INT64,
            descricao_tipo_pessoa STRING,
            nome STRING
        >>,
        possui_imovel_associado BOOL,
        imovel_associado STRUCT<
            endereco STRUCT<
                codigo_logradouro INT64,
                nome_logradouro STRING,
                numero_porta STRING,
                complemento_endereco STRING,
                bairro STRING,
                cep STRING
            >,
            tipologia_imovel STRUCT<
                id_tipologia_imovel INT64,
                nome_tipologia_imovel STRING
            >,
            utilizacao_imovel STRUCT<
                id_utilizacao_imovel INT64,
                nome_utilizacao_imovel STRING
            >
        >
    >>,
    quantidade_guias INT64,
    valor_cotas_devidas_a_vencer NUMERIC,
    quantidade_cotas_devidas_a_vencer INT64,
    valor_cotas_devidas_vencidas NUMERIC,
    quantidade_cotas_devidas_vencidas INT64,
    valor_cotas_pagas NUMERIC,
    quantidade_cotas_pagas INT64,
    guias_pagamento ARRAY<STRUCT<
        id_guia_pagamento INT64,
        data_criacao_guia DATETIME,
        numero_processo_associado STRING,
        tipo_guia STRUCT<
            id_tipo_guia INT64,
            nome_tipo_guia STRING
        >,
        tipo_pagamento STRUCT<
            id_tipo_pagamento INT64,
            nome_tipo_pagamento STRING
        >,
        tipo_receita STRUCT<
            codigo_receita STRING,
            nome_receita STRING
        >,
        situacao STRUCT<
            id_situacao_guia_pagamento INT64,
            nome_situacao_guia_pagamento STRING
        >,
        quantidade_cotas INT64,
        cotas ARRAY<STRUCT<
            id_cota_guia_pagamento INT64,
            cota_paga BOOL,
            cota_substituida BOOL,
            substituta_de_outra BOOL,
            valor_cota_guia_pagamento NUMERIC,
            data_emissao DATETIME,
            data_vencimento DATE,
            data_pagamento DATETIME,
            valor_principal NUMERIC,
            valor_honorarios NUMERIC,
            valor_juros NUMERIC,
            valor_grerj NUMERIC,
            valor_juros_principal NUMERIC,
            valor_juros_honorarios NUMERIC,
            observacoes STRING,
            valor_principal_pago NUMERIC,
            valor_juros_pago NUMERIC,
            valor_honorarios_pago NUMERIC,
            valor_grerj_pago NUMERIC,
            valor_juros_honorarios_pago NUMERIC,
            ano_ipcae INT64,
            codigo_barras STRING,
            id_pix STRING,
            codigo_qr_pix STRING
        >>
    >>
);

-- ===================== 3) rj-crm-registry.rmi_dados_mestres.pessoa_fisica =====================
CREATE OR REPLACE TABLE `rj-crm-registry-dev.dev__dev_fantasma__rmi_dados_mestres.pessoa_fisica` (
    cpf STRING,
    nome STRING,
    nome_social STRING,
    sexo STRING,
    nascimento STRUCT<
        data DATE,
        municipio_id STRING,
        municipio STRING,
        uf STRING,
        pais_id STRING,
        pais STRING
    >,
    mae STRUCT<
        nome STRING,
        cpf STRING
    >,
    menor_idade BOOL,
    raca STRING,
    obito STRUCT<
        indicador BOOL,
        ano INT64
    >,
    documentos STRUCT<
        cns ARRAY<STRING>
    >,
    endereco STRUCT<
        indicador BOOL,
        principal STRUCT<
            origem STRING,
            sistema STRING,
            cep STRING,
            estado STRING,
            municipio STRING,
            tipo_logradouro STRING,
            logradouro STRING,
            numero STRING,
            complemento STRING,
            bairro STRING,
            latitude STRING,
            longitude STRING,
            pluscode STRING,
            data_atualizacao DATE
        >,
        alternativo ARRAY<STRUCT<
            origem STRING,
            sistema STRING,
            cep STRING,
            estado STRING,
            municipio STRING,
            tipo_logradouro STRING,
            logradouro STRING,
            numero STRING,
            complemento STRING,
            bairro STRING,
            latitude STRING,
            longitude STRING,
            pluscode STRING,
            data_atualizacao DATE
        >>
    >,
    email STRUCT<
        indicador BOOL,
        principal STRUCT<
            origem STRING,
            sistema STRING,
            valor STRING,
            data_atualizacao DATE
        >,
        alternativo ARRAY<STRUCT<
            origem STRING,
            sistema STRING,
            valor STRING,
            data_atualizacao DATE
        >>
    >,
    telefone STRUCT<
        indicador BOOL,
        principal STRUCT<
            origem STRING,
            sistema STRING,
            ddi STRING,
            ddd STRING,
            valor STRING,
            qualidade STRING,
            confianca STRING,
            tipo STRING,
            estrategia_envio STRING,
            indicador_optin BOOL,
            indicador_optout BOOL,
            indicador_soft_optin BOOL,
            indicador_quarentena BOOL,
            data_fim_quarentena DATE,
            consentimento STRING,
            razao_optout STRING,
            datahora_optin DATETIME,
            datahora_optout DATETIME,
            datahora_ultima_leitura DATETIME,
            datahora_ultima_resposta DATETIME,
            data_atualizacao DATE
        >,
        alternativo ARRAY<STRUCT<
            origem STRING,
            sistema STRING,
            ddi STRING,
            ddd STRING,
            valor STRING,
            qualidade STRING,
            confianca STRING,
            tipo STRING,
            estrategia_envio STRING,
            indicador_optin BOOL,
            indicador_optout BOOL,
            indicador_soft_optin BOOL,
            indicador_quarentena BOOL,
            data_fim_quarentena DATE,
            consentimento STRING,
            razao_optout STRING,
            datahora_optin DATETIME,
            datahora_optout DATETIME,
            datahora_ultima_leitura DATETIME,
            datahora_ultima_resposta DATETIME,
            data_atualizacao DATE
        >>
    >,
    assistencia_social STRUCT<
        cras STRUCT<
            id STRING,
            nome STRING,
            endereco STRING,
            telefone STRING
        >,
        cadunico STRUCT<
            indicador BOOL,
            id_familia STRING,
            id_membro_familia STRING,
            data_cadastro DATE,
            data_ultima_atualizacao DATE,
            data_limite_cadastro_atual DATE,
            status_cadastral STRING,
            renda_familiar_per_capita FLOAT64,
            responsavel_familiar STRUCT<
                indicador BOOL,
                cpf STRING,
                id_membro_familia STRING,
                nome STRING,
                parentesco_com_responsavel STRING
            >
        >,
        programa STRUCT<
            bolsa_familia STRUCT<
                indicador BOOL,
                status STRING,
                beneficio_familiar_valor NUMERIC,
                beneficio_familiar_faixa STRING
            >,
            cartao_pic STRUCT<
                indicador BOOL,
                entrega_status STRING,
                entrega_indicador BOOL,
                entrega_data_prevista DATE
            >
        >
    >,
    educacao STRUCT<
        indicador BOOL,
        matricula STRUCT<
            id_aluno STRING,
            situacao STRING
        >,
        turma STRUCT<
            id STRING,
            nivel_ensino STRING,
            grupamento STRING,
            turno STRING
        >,
        escola STRUCT<
            id STRING,
            nome STRING,
            id_cre STRING,
            id_inep STRING,
            tipo STRING,
            horario_funcionamento STRING,
            endereco STRING,
            email STRING,
            whatsapp STRING,
            telefone STRING
        >,
        frequencia_escolar_percentual FLOAT64
    >,
    saude STRUCT<
        clinica_familia STRUCT<
            indicador BOOL,
            id_cnes STRING,
            nome STRING,
            telefone STRING,
            email STRING,
            endereco STRING,
            horario_atendimento_dia_util STRING,
            horario_atendimento_sabado STRING,
            area_programatica STRING
        >,
        equipe_saude_familia STRUCT<
            indicador BOOL,
            id_ine STRING,
            nome STRING,
            telefone STRING,
            medicos ARRAY<STRUCT<
                id_profissional_sus STRING,
                nome STRING
            >>,
            enfermeiros ARRAY<STRUCT<
                id_profissional_sus STRING,
                nome STRING
            >>
        >,
        tem_atendimento_aps BOOL,
        condicao STRUCT<
            gravidez STRUCT<
                indicador BOOL,
                data_inicio_estimada DATE
            >,
            puerperio STRUCT<
                indicador BOOL,
                data_inicio_estimada DATE
            >
        >
    >,
    ocupacao STRUCT<
        trabalha_prefeitura STRUCT<
            indicador BOOL,
            vinculo_ativo BOOL
        >
    >,
    datalake STRUCT<
        last_updated TIMESTAMP
    >,
    cpf_particao INT64
);
