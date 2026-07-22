-- Teste da query queries_dev/cvl_pesquisa_1746.sql (mirror de queries/cvl_pesquisa_1746.sql)
-- Placeholders já substituídos com os valores do schedule "daily-cvl-pesquisa1746-unificado"
-- (scheduler_sf.yaml): id_hsm_com_solucao_placeholder -> 192, nome_hsm_com_solucao_placeholder ->
-- 'cvl_pesquisa_1746_prod_v1', id_hsm_sem_resolucao_placeholder -> 291,
-- nome_hsm_sem_resolucao_placeholder -> 'cvl_pesquisa_1746_sem_resolucao_prod_v1'.
-- Executa direto no BigQuery. Passos 1 a 5 inserem dois chamados 1746 de teste (um fechado
-- com solução, outro sem resolução) que caem em todos os filtros da query; passo 6 é a query
-- em si, adaptada dos datasets dev__dev_fantasma__. Sem insert na wetalkie
-- (fluxo_atendimento_*) de propósito: o LEFT JOIN legado fica inerte, só exercitamos o
-- caminho novo (status_disparo). Expectativa: 2 linhas, uma com STATUS_DEMANDA = 'ATENDIDA'
-- e outra com STATUS_DEMANDA = 'SEM_RESOLUCAO'.

-- ===================== 0) LIMPA DADOS DE TESTE ANTERIORES =====================
DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado`
WHERE id_chamado IN ('TESTE1746COMSOL01', 'TESTE1746SEMRES01');

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado_cpf`
WHERE id_chamado IN ('TESTE1746COMSOL01', 'TESTE1746SEMRES01');

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.pessoa`
WHERE id_pessoa IN (999991746, 999991747);

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.origem_ocorrencia`
WHERE id_origem_ocorrencia IN ('17', '11');

DELETE FROM `rj-crm-registry-dev.dev__dev_fantasma__brutos_extracoes_google_sheets.relacao_bairro_subprefeitura`
WHERE id_bairro IN ('TESTE1746BAIRROCS', 'TESTE1746BAIRROSR');

-- garante que os cpfs de teste não aparecem como já disparados hoje (dados_finais exige sd.cpf IS NULL)
DELETE FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE (cpf = '12cvlcsol01' AND nome_hsm = 'cvl_pesquisa_1746_prod_v1')
   OR (cpf = '12cvlsres01' AND nome_hsm = 'cvl_pesquisa_1746_sem_resolucao_prod_v1');

-- ===================== 1) INSERT: chamados de teste (com solução + sem resolução) =====================
-- data_fim é fixo em "ontem", mesma regra usada pela query (sem o gate de fim de semana que
-- só existe na versão scheduler.yaml/Wetalkie), assim o teste passa em qualquer dia da semana.
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado`
(id_chamado, id_origem_ocorrencia, data_inicio, data_fim, id_bairro, categoria, id_tipo, tipo, id_subtipo, subtipo, status)
VALUES
(
    'TESTE1746COMSOL01', '17', DATETIME '2025-06-01 09:00:00',
    DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
    'TESTE1746BAIRROCS', 'Serviço', '1', 'Fiscalização', '2966',
    'Fiscalização de estacionamento irregular de veículo', 'Fechado com solução'
),
(
    'TESTE1746SEMRES01', '11', DATETIME '2025-06-01 09:00:00',
    DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
    'TESTE1746BAIRROSR', 'Serviço', '1', 'Fiscalização', '2966',
    'Fiscalização de estacionamento irregular de veículo', 'Não constatado'
);

-- ===================== 2) INSERT: vínculo chamado x cpf =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado_cpf`
(id_chamado, cpf, id_pessoa, numero_protocolo, ic_motivo)
VALUES
('TESTE1746COMSOL01', '12cvlcsol01', '999991746', '20260713000001', 'E'),
('TESTE1746SEMRES01', '12cvlsres01', '999991747', '20260713000002', 'E');

-- ===================== 3) INSERT: pessoas de teste =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.pessoa`
(id_pessoa, cpf, nome, telefone_1)
VALUES
(999991746, '12cvlcsol01', 'MARIA SALESFORCE CVL SOLUCAO', '5521989190512'),
(999991747, '12cvlsres01', 'JOAO SALESFORCE CVL SEMRESOL', '5521989190513');

-- ===================== 4) INSERT: origens de ocorrência de teste =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.origem_ocorrencia`
(id_origem_ocorrencia, no_origem_ocorrencia)
VALUES
('17', 'WhatsApp 1746'),
('11', 'Aplicativo 1746');

-- ===================== 5) INSERT: bairros/subprefeituras de teste =====================
INSERT INTO `rj-crm-registry-dev.dev__dev_fantasma__brutos_extracoes_google_sheets.relacao_bairro_subprefeitura`
(bairro, id_bairro, no_subprefeitura)
VALUES
('TESTE SALESFORCE 1746 COM SOLUCAO', 'TESTE1746BAIRROCS', 'Zona Teste'),
('TESTE SALESFORCE 1746 SEM RESOLUCAO', 'TESTE1746BAIRROSR', 'Zona Teste');

-- ===================== 6) QUERY (queries_dev/cvl_pesquisa_1746.sql com placeholders substituídos) =====================
WITH tabela_global AS (
    SELECT
        DISTINCT
        t1.id_chamado,
        t3.id_protocolo,
        t3.id_protocolo_chamado,
        t3.numero_protocolo,
        t3.ic_motivo,
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
        t3.cpf,
        t4.id_pessoa,
        t4.nome,
        CONCAT(
        INITCAP(SPLIT(t4.nome, ' ')[OFFSET(0)]),
        ' ',
        INITCAP(SPLIT(t4.nome, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(t4.nome, ' ')) - 1)])
        ) AS nome_tratado,
        t4.email,
        COALESCE(
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_1, r'[^\d]', '')),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_2, r'[^\d]', '')),
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(t4.telefone_3, r'[^\d]', ''))
        ) AS telefone,
        t1.data_inicio,
        t1.data_fim,
        t1.data_alvo_finalizacao,
        t1.categoria,
        t1.id_tipo,
        t1.tipo,
        t1.id_subtipo,
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
        t1.subtipo,
        t1.id_territorialidade,
        t1.id_unidade_organizacional,
        t1.nome_unidade_organizacional,
        t1.id_bairro,
        t2.bairro,
        t2.no_subprefeitura AS subprefeitura,
        t1.id_logradouro,
        t1.numero_logradouro,
        t1.status,
        t1.situacao,
        t1.tipo_situacao,
        t1.tempo_prazo,
        CASE
        WHEN t1.status IN ('Fechado com providências', 'Fechado com solução') THEN 'ATENDIDA'
        WHEN t1.status IN ('Sem possibilidade de atendimento', 'Fechado com informação', 'Não constatado') THEN 'SEM_RESOLUCAO'
        END AS status_demanda_tratado
    FROM `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado` t1
    LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__brutos_extracoes_google_sheets.relacao_bairro_subprefeitura` t2
        ON t1.id_bairro = t2.id_bairro
    LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.chamado_cpf` t3
        ON t1.id_chamado = t3.id_chamado
    LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.pessoa` t4
        ON t3.id_pessoa = CAST(t4.id_pessoa AS STRING)
    LEFT JOIN `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746.origem_ocorrencia` t5
        ON t1.id_origem_ocorrencia = CAST(t5.id_origem_ocorrencia AS STRING)
    WHERE 1=1
        AND t1.data_inicio >= '2025-01-01 00:00:00.000'
        AND (
            t1.id_subtipo IN ('5899', '3366')
            OR
            (
            t1.id_subtipo IN (
            '2966', '1325', '5799', '78', '100', '1298', '1319', '1316', '1274', '1242', '1287', '5071', '1279', '2178', '3139', '114', '5841', '1101' )
            AND t1.categoria IN ("Serviço")
            )
        )
        AND t1.data_fim IS NOT NULL
        AND t1.id_bairro IS NOT NULL
        AND t1.id_origem_ocorrencia NOT IN ('23','25','27')
        AND COALESCE(t4.telefone_1, t4.telefone_2, t4.telefone_3) IS NOT NULL
        AND t3.ic_motivo IN ('E','O')
        AND t1.status IN (
            'Fechado com providências', 'Fechado com solução',
            'Sem possibilidade de atendimento', 'Fechado com informação', 'Não constatado'
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
                    WHEN 'ATENDIDA' THEN 192
                    WHEN 'SEM_RESOLUCAO' THEN 291
                END AS int64)
            and date(fl.createDate) = CURRENT_DATE("America/Sao_Paulo") and fl.status="PROCESSING"
    left join `rj-crm-registry.brutos_salesforce.status_disparo` sd
            on sd.cpf = ta.cpf
            and sd.nome_hsm = CASE ta.status_demanda_tratado
                    WHEN 'ATENDIDA' THEN 'cvl_pesquisa_1746_prod_v1'
                    WHEN 'SEM_RESOLUCAO' THEN 'cvl_pesquisa_1746_sem_resolucao_prod_v1'
                END
            and DATE(sd.envio_datahora) = CURRENT_DATE("America/Sao_Paulo")
            and sd.data_particao = CURRENT_DATE("America/Sao_Paulo")
            and sd.indicador_quarentena = FALSE
    WHERE fl.flattarget is null
        AND sd.cpf is null
        AND DATE(data_fim) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND telefone IS NOT NULL
    )

    SELECT
        telefone,
        CAST(cpf AS STRING) AS SubscriberKey,
        CAST(cpf AS STRING) AS externalId,
        CAST(cpf AS STRING) AS cpfCnpj,
        nome AS cc_wt_nome_sobrenome,
        subtipo_tratado AS cc_wt_solicitacao,
        canal_tratado AS cc_wt_canal,
        status_demanda_tratado AS STATUS_DEMANDA
    FROM dados_finais
    ORDER BY STATUS_DEMANDA;
