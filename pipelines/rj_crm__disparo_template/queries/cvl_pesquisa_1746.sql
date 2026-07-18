-- Query unificada: pesquisa de satisfação CVL 1746 (fechado com solução + sem resolução).
-- Cada chamado só cai em um dos dois desfechos, e status_demanda_tratado carrega qual foi
-- pra escolher, por linha, o par id_hsm/nome_hsm certo nos LEFT JOINs de dedup abaixo.
WITH tabela_global AS (
    SELECT
        DISTINCT
        -- Identificação do chamado e protocolo
        t1.id_chamado,
        t3.id_protocolo,
        t3.id_protocolo_chamado,
        t3.numero_protocolo,
        t3.ic_motivo,

        -- Descrição e canal de origem
        t1.descricao AS descricao_chamado,
        t5.no_origem_ocorrencia AS canal_atendimento,
        CASE
        WHEN t1.id_origem_ocorrencia = '1' THEN 'Central 1746'
        WHEN t1.id_origem_ocorrencia = '11' THEN 'Aplicativo 1746'
        WHEN t1.id_origem_ocorrencia = '12' THEN 'Agência 1746'
        WHEN t1.id_origem_ocorrencia = '13' THEN 'Site 1746'
        WHEN t1.id_origem_ocorrencia = '16' THEN 'Carioca Digital'
        WHEN t1.id_origem_ocorrencia = '17' THEN 'WhatsApp 1746'
        WHEN t1.id_origem_ocorrencia = '18' THEN 'Van 1746'
        WHEN t1.id_origem_ocorrencia = '26' THEN 'Agência 1746'
        WHEN t1.id_origem_ocorrencia = '28' THEN 'WhatsApp 1746'
        ELSE t1.id_origem_ocorrencia
        END AS canal_tratado,

        -- Dados do cidadão
        t3.cpf,
        t4.id_pessoa,
        t4.nome,
        CONCAT(
        INITCAP(SPLIT(t4.nome, ' ')[OFFSET(0)]),
        ' ',
        INITCAP(SPLIT(t4.nome, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(t4.nome, ' ')) - 1)])
        ) AS nome_tratado,
        t4.email,

        -- Telefones limpos e formatados
        COALESCE(
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_1, r'[^\d]', '')),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_2, r'[^\d]', '')),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_3, r'[^\d]', ''))
        ) AS telefone,

        -- Datas e informações operacionais
        t1.data_inicio,
        t1.data_fim,
        t1.data_alvo_finalizacao,
        t1.categoria,
        t1.id_tipo,
        t1.tipo,
        t1.id_subtipo,

        -- Subtipo tratado (descrição legível)
        CASE
        WHEN t1.id_subtipo = '1274' THEN 'Verificação de frequência irregular da coleta domiciliar'
        WHEN t1.id_subtipo = '1242' THEN 'Verificação de ar condicionado inoperante no ônibus'
        WHEN t1.id_subtipo = '1287' THEN 'Varrição de logradouro'
        WHEN t1.id_subtipo = '2178' THEN 'Reposição de tampão ou grelha'
        WHEN t1.id_subtipo = '3139' THEN 'Reparo de sinal de trânsito apagado'
        WHEN t1.id_subtipo = '5799' THEN 'Reparo de Luminária'
        WHEN t1.id_subtipo = '78' THEN 'Reparo de buraco na pista'
        WHEN t1.id_subtipo = '1298' THEN 'Remoção de resíduos no logradouro'
        WHEN t1.id_subtipo = '1325' THEN 'Remoção de entulho e bens inservíveis'
        WHEN t1.id_subtipo = '1319' THEN 'Poda de árvore em logradouro'
        WHEN t1.id_subtipo = '5071' THEN 'Fiscalização de perturbação do sossego'
        WHEN t1.id_subtipo = '114' THEN 'Fiscalização de obstáculo fixo na calçada'
        WHEN t1.id_subtipo = '2966' THEN 'Fiscalização de estacionamento irregular de veículo'
        WHEN t1.id_subtipo = '1101' THEN 'Fiscalização de comércio ambulante'
        WHEN t1.id_subtipo = '5841' THEN 'Fiscalização da ocupação de área pública'
        WHEN t1.id_subtipo = '100' THEN 'Desobstrução de ramais e ralos'
        WHEN t1.id_subtipo = '1316' THEN 'Controle de roedores e caramujos africanos'
        WHEN t1.id_subtipo = '1279' THEN 'Capina em logradouro'
        WHEN t1.id_subtipo = '5899' THEN 'Informação sobre Fiscalização de comércio ambulante'
        WHEN t1.id_subtipo = '3366' THEN 'Comércio Ambulante'
        ELSE t1.subtipo
        END AS subtipo_tratado,

        -- Localização e unidade responsável
        t1.subtipo,
        t1.id_territorialidade,
        t1.id_unidade_organizacional,
        t1.nome_unidade_organizacional,
        t1.id_bairro,
        t2.bairro,
        t2.no_subprefeitura AS subprefeitura,
        t1.id_logradouro,
        t1.numero_logradouro,

        -- Status e tempo de atendimento
        t1.status,
        t1.situacao,
        t1.tipo_situacao,
        t1.tempo_prazo,

        -- Desfecho tratado: define qual dos dois disparos (HSM) esse chamado deve receber
        CASE
        WHEN t1.status IN ('Fechado com providências', 'Fechado com solução') THEN 'ATENDIDA'
        WHEN t1.status IN ('Sem possibilidade de atendimento', 'Fechado com informação', 'Não constatado') THEN 'SEM_RESOLUCAO'
        END AS status_demanda_tratado

    FROM rj-segovi.adm_central_atendimento_1746.chamado t1
    LEFT JOIN rj-iplanrio.brutos_extracoes_google_sheets.relacao_bairro_subprefeitura t2
        ON t1.id_bairro = t2.id_bairro
    LEFT JOIN rj-segovi.adm_central_atendimento_1746.chamado_cpf t3
        ON t1.id_chamado = t3.id_chamado
    LEFT JOIN rj-segovi.adm_central_atendimento_1746.pessoa t4
        ON t3.id_pessoa = CAST(t4.id_pessoa AS STRING)
    LEFT JOIN rj-segovi.adm_central_atendimento_1746.origem_ocorrencia t5
        ON t1.id_origem_ocorrencia = CAST(t5.id_origem_ocorrencia AS STRING)
    WHERE 1=1
        AND t1.data_inicio >= '2025-01-01 00:00:00.000' -- Considera somente chamados a partir de Jan/25
        AND (
            -- Aceitam qualquer categoria
            t1.id_subtipo IN ('5899', '3366')
            OR
            -- Exigem ser "Serviço"
            (
            t1.id_subtipo IN (
            '2966', '1325', '5799', '78', '100', '1298', '1319', '1316', '1274', '1242', '1287', '5071', '1279', '2178', '3139', '114', '5841', '1101' )
            AND t1.categoria IN ("Serviço")
            )
        ) --Considera somente 20 serviços selecionados pela SUBTD
        AND t1.data_fim IS NOT NULL -- Somente chamados fechados
        AND t1.id_bairro IS NOT NULL -- Somente chamados com bairro classificado
        AND t1.id_origem_ocorrencia NOT IN ('23','25','27') -- Exclui Táxi.Rio, Facebook e Conservação
        AND COALESCE(t4.telefone_1, t4.telefone_2, t4.telefone_3) IS NOT NULL -- Exclui sem telefone
        AND t3.ic_motivo IN ('E','O') -- Apenas ocorrências e equivalências
        AND t1.status IN (
            'Fechado com providências', 'Fechado com solução', -- ATENDIDA
            'Sem possibilidade de atendimento', 'Fechado com informação', 'Não constatado' -- SEM_RESOLUCAO
        )
    ),

    dados_finais AS (
    SELECT
        numero_protocolo,
        canal_tratado,
        ta.cpf,
        nome_tratado AS nome,
        telefone,
        data_inicio,
        data_fim,
        subtipo_tratado,
        id_subtipo,
        ta.status,
        ta.status_demanda_tratado
    FROM tabela_global ta
    left join `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
            on fl.flattarget = ta.telefone
            and fl.templateId = CAST(
                CASE ta.status_demanda_tratado
                    WHEN 'ATENDIDA' THEN {id_hsm_com_solucao_placeholder}
                    WHEN 'SEM_RESOLUCAO' THEN {id_hsm_sem_resolucao_placeholder}
                END AS int64)
            and date(fl.createDate) = CURRENT_DATE("America/Sao_Paulo") and fl.status="PROCESSING"
    -- verifica também se esse cpf já recebeu essa mesma mensagem (nome_hsm correspondente ao
    -- desfecho) hoje, via status_disparo (alimentada pelos disparos via SFTP/Salesforce)
    left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
            on sd.cpf = ta.cpf
            and sd.nome_hsm = CASE ta.status_demanda_tratado
                    WHEN 'ATENDIDA' THEN '{nome_hsm_com_solucao_placeholder}'
                    WHEN 'SEM_RESOLUCAO' THEN '{nome_hsm_sem_resolucao_placeholder}'
                END
            and DATE(sd.envio_datahora) = CURRENT_DATE("America/Sao_Paulo")
            and sd.data_particao = CURRENT_DATE("America/Sao_Paulo")
            and sd.indicador_quarentena = FALSE
    WHERE fl.flattarget is null
        AND sd.cpf is null
        AND DATE(data_fim) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND telefone IS NOT NULL
    )

    -- Tabela simples (sem TO_JSON_STRING). 'externalId' é controle interno (dedup por
    -- CPF) e é descartado do CSV pelo de_columns antes do envio à Data Extension.
    SELECT
        telefone,
        CAST(cpf AS STRING) AS SubscriberKey,
        CAST(cpf AS STRING) AS externalId,
        CAST(cpf AS STRING) AS cpfCnpj,
        nome AS cc_wt_nome_sobrenome,
        subtipo_tratado AS cc_wt_solicitacao,
        canal_tratado AS cc_wt_canal,
        status_demanda_tratado AS STATUS_DEMANDA
    FROM dados_finais;
