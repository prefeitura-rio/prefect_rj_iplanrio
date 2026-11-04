-- ============================================================
-- Holdout - Multas vencidas / não pagas (SMTR)
--
-- Todo dia útil:
--   1) monta a base do dia e tira quem foi sorteado há menos de 40 dias, de qualquer
--      grupo (janela do desfecho de 30 dias + 10 de folga); no dia D+40 a pessoa volta
--      para a fila e pode ser sorteada de novo, numa coorte nova, em qualquer grupo;
--   2) seleciona 2 × cota_diaria em ordem aleatória (FARM_FINGERPRINT(cpf || data));
--   3) embaralha os selecionados e divide exatamente 50/50 (par = controle);
--   4) insere a coorte do dia em smtr_multa_atribuicao, só se ela ainda não existir
--      (o job é idempotente: rodar de novo no mesmo dia não sorteia de novo);
--   5) devolve a lista de disparo só com o tratamento do dia, lida da tabela.
--
-- O controle não pode receber nenhuma mensagem até fim_quarentena. Todo outro
-- disparo ou jornada que atinja esse público precisa filtrar:
--   cpf NOT IN (SELECT id_devedor FROM `rj-crm-registry.ab_test.smtr_multa_atribuicao`
--               WHERE grupo = 'controle' AND CURRENT_DATE("America/Sao_Paulo") < fim_quarentena)
--
-- Fica fora do experimento (nem tratamento nem controle): homens cujo veículo da
-- multa mais recente é moto, já comprovados de dar prejuízo. A regra é a mesma da
-- análise (multas_resultados_ab/queries/atributos_por_cpf.sql): tipo_veiculo da
-- autuação mais recente do CPF, entre todas as multas, pagas ou não.
-- ============================================================


DECLARE hoje DATE DEFAULT CURRENT_DATE("America/Sao_Paulo");
DECLARE dia_util BOOL DEFAULT EXTRACT(DAYOFWEEK FROM hoje) NOT IN (1, 7);
DECLARE cota_diaria INT64 DEFAULT CAST({cota_diaria_placeholder} AS INT64);  -- tratamento por dia; seleciona 2× (metade vai pro controle)
DECLARE dias_quarentena INT64 DEFAULT CAST({dias_quarentena_placeholder} AS INT64);  -- janela do desfecho (30 dias) + folga
DECLARE max_infracoes_veiculo INT64 DEFAULT CAST({max_infracoes_veiculo_placeholder} AS INT64);  -- veículo principal com mais que isso fica fora


CREATE TABLE IF NOT EXISTS `{tabela_atribuicao_placeholder}` (
    id_devedor STRING NOT NULL,
    data_selecao DATE NOT NULL,
    grupo STRING NOT NULL,
    fim_quarentena DATE,
    ordem_selecao INT64,
    ordem_grupo INT64,
    celular_disparo STRING,
    nome_proprietario STRING,
    sexo STRING,
    idade INT64,
    faixa_idade STRING,
    bairro STRING,
    qualidade_tel STRING,
    estrategia_envio STRING,
    tipo_veiculo_recente STRING,
    descricao_veiculo STRING,
    placa_veiculo STRING,
    id_autuacao STRING,
    n_infracoes_total INT64,
    valor_total_infracoes NUMERIC,
    n_veiculos_com_infracao INT64,
    n_infracoes_veiculo_principal INT64,
    valor_total_veiculo_principal NUMERIC,
    todos_id_auto_infracao STRING,
    primeira_data_autuacao DATE,
    ultima_data_autuacao DATE,
    n_atribuicoes_anteriores INT64,
    inserido_em TIMESTAMP
)
PARTITION BY data_selecao
CLUSTER BY grupo, id_devedor;


-- ============================================================
-- 1) RMI: dados demográficos e telefone
--    Sem óbito, sem menor de idade e sem estratégia NÃO ENVIAR
-- ============================================================
CREATE TEMP TABLE rmi AS
WITH rmi_base AS (
    SELECT
        LPAD(REGEXP_REPLACE(CAST(pf.cpf AS STRING), r'\D', ''), 11, '0') AS cpf,
        COALESCE(pf.nome_social, pf.nome) AS nome_completo,
        `rj-crm-registry.udf.FORMAT_NAME`(COALESCE(pf.nome_social, pf.nome), TRUE) AS primeiro_nome,
        pf.sexo,
        DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) AS idade,
        CASE
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) BETWEEN 18 AND 29 THEN '18-29'
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) BETWEEN 30 AND 39 THEN '30-39'
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) BETWEEN 40 AND 49 THEN '40-49'
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) BETWEEN 50 AND 59 THEN '50-59'
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) BETWEEN 60 AND 69 THEN '60-69'
            WHEN DATE_DIFF(hoje, DATE(pf.nascimento.data), YEAR) >= 70 THEN '70+'
            ELSE 'Idade não informada'
        END AS faixa_idade,
        pf.endereco.principal.bairro AS bairro,
        pf.telefone.principal.qualidade AS qualidade_tel,
        pf.telefone.principal.estrategia_envio AS estrategia_envio,
        `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(
            CONCAT(
                IFNULL(pf.telefone.principal.ddi, "55"),
                IFNULL(pf.telefone.principal.ddd, "21"),
                pf.telefone.principal.valor
            )
        ) AS celular_disparo
    FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` pf
    WHERE pf.cpf IS NOT NULL
        AND pf.telefone.principal.estrategia_envio != 'NÃO ENVIAR'
        AND pf.obito.indicador IS FALSE
        AND pf.menor_idade IS FALSE
)

SELECT *
FROM rmi_base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cpf
    ORDER BY
        CASE
            WHEN estrategia_envio = 'ENVIAR' THEN 0
            WHEN estrategia_envio = 'TESTAR' THEN 1
            WHEN estrategia_envio = 'EVITAR' THEN 2
            WHEN estrategia_envio = 'NÃO ENVIAR' THEN 3
            ELSE 9
        END,
        CASE WHEN qualidade_tel = 'VALIDO' THEN 0 ELSE 1 END,
        cpf
) = 1;


-- ============================================================
-- 2) Multas vencidas / não pagas
-- ============================================================
/*
    Extração da lista de multas vencidas desde 2025 (PF)
    Regras de negócio:
      1. Recorte temporal: autuações desde 2025
      2. Situação: "Ativo" ou "Desvinculado" e "NP Gerada"
      3. Sem pagamento: valor_pago e data_pagamento nulos
      4. Sem defesa/recurso: processo_defesa_autuacao e recurso_penalidade_multa nulos
      5. Precedência: proprietário; na falta, possuidor
      6. Filtro PF: documento com 11 dígitos
      7. Vencidas: data_limite_recurso < hoje
*/
CREATE TEMP TABLE multas_detalhadas AS
WITH autuacao_vencida_raw AS (
    SELECT
        REGEXP_REPLACE(
            CAST(COALESCE(documento_proprietario, documento_possuidor_veiculo) AS STRING),
            r'\D',
            ''
        ) AS cpf_raw,
        descricao_veiculo,
        placa_veiculo,
        CAST(id_auto_infracao AS STRING) AS id_auto_infracao,
        SAFE_CAST(valor_infracao AS NUMERIC) AS valor_infracao,
        data AS data_autuacao
    FROM `rj-smtr.transito.autuacao`
    WHERE data BETWEEN DATE '2025-01-01' AND hoje
        AND status_infracao = "NP Gerada"
        AND descricao_situacao_autuacao IN ('Ativo', 'Desvinculado')
        AND data_pagamento IS NULL
        AND COALESCE(valor_pago, 0) = 0
        AND processo_defesa_autuacao IS NULL
        AND recurso_penalidade_multa IS NULL
        AND data_limite_recurso < hoje
)

SELECT
    LPAD(cpf_raw, 11, '0') AS cpf,
    descricao_veiculo,
    placa_veiculo,
    id_auto_infracao,
    valor_infracao,
    data_autuacao
FROM autuacao_vencida_raw
WHERE LENGTH(cpf_raw) = 11
    AND cpf_raw != '00000000000'
    AND valor_infracao IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cpf_raw, id_auto_infracao
    ORDER BY data_autuacao DESC
) = 1;


-- ============================================================
-- 3) Tipo do veículo da multa mais recente, entre todas as multas do CPF
--    (pagas ou não), igual a atributos_por_cpf.sql da análise
-- ============================================================
CREATE TEMP TABLE veiculo_recente AS
WITH todas_autuacoes AS (
    SELECT
        LPAD(
            REGEXP_REPLACE(
                CAST(COALESCE(documento_proprietario, documento_possuidor_veiculo) AS STRING),
                r'\D',
                ''
            ),
            11,
            '0'
        ) AS cpf,
        id_auto_infracao,
        data AS data_autuacao,
        tipo_veiculo
    FROM `rj-smtr.transito.autuacao`
    WHERE data BETWEEN DATE '2023-01-01' AND hoje
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf, id_auto_infracao
        ORDER BY datetime_ultima_atualizacao DESC
    ) = 1
)

SELECT
    cpf,
    ARRAY_AGG(tipo_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS tipo_veiculo_recente
FROM todas_autuacoes
WHERE cpf IN (SELECT DISTINCT cpf FROM multas_detalhadas)
GROUP BY cpf;


-- ============================================================
-- 4) Agregado por CPF
-- ============================================================
CREATE TEMP TABLE multas_por_cpf AS
SELECT
    cpf,
    COUNT(DISTINCT id_auto_infracao) AS n_infracoes_total,
    SUM(valor_infracao) AS valor_total_infracoes,
    COUNT(DISTINCT placa_veiculo) AS n_veiculos_com_infracao,
    STRING_AGG(DISTINCT id_auto_infracao, ', ' ORDER BY id_auto_infracao) AS todos_id_auto_infracao,
    MIN(data_autuacao) AS primeira_data_autuacao,
    MAX(data_autuacao) AS ultima_data_autuacao
FROM multas_detalhadas
GROUP BY cpf;


-- ============================================================
-- 5) Veículo principal: o de maior número de infrações não pagas por CPF
-- ============================================================
CREATE TEMP TABLE veiculo_principal AS
WITH veiculo_cpf AS (
    SELECT
        cpf,
        COALESCE(placa_veiculo, 'SEM_PLACA') AS placa_veiculo,
        COALESCE(descricao_veiculo, 'SEM_DESCRICAO') AS descricao_veiculo,
        COUNT(DISTINCT id_auto_infracao) AS n_infracoes_veiculo,
        SUM(valor_infracao) AS valor_total_veiculo,
        STRING_AGG(DISTINCT id_auto_infracao, ', ' ORDER BY id_auto_infracao) AS ids_auto_infracao_veiculo
    FROM multas_detalhadas
    GROUP BY cpf, placa_veiculo, descricao_veiculo
)

SELECT
    cpf,
    descricao_veiculo,
    placa_veiculo,
    ids_auto_infracao_veiculo AS id_autuacao,
    n_infracoes_veiculo AS n_infracoes_veiculo_principal,
    valor_total_veiculo AS valor_total_veiculo_principal
FROM veiculo_cpf
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cpf
    ORDER BY n_infracoes_veiculo DESC, valor_total_veiculo DESC, placa_veiculo, descricao_veiculo
) = 1;


-- ============================================================
-- 6) Base do dia
--    Remove CPFs sem nome, sem celular, veículos principais com mais de max_infracoes_veiculo infrações,
--    homens com moto e quem está com janela aberta no experimento
-- ============================================================
CREATE TEMP TABLE base_do_dia AS
SELECT
    m.cpf,
    r.nome_completo AS nome_proprietario,
    r.sexo,
    r.idade,
    r.faixa_idade,
    r.bairro,
    r.qualidade_tel,
    r.estrategia_envio,
    r.celular_disparo,
    vr.tipo_veiculo_recente,
    v.descricao_veiculo,
    v.placa_veiculo,
    v.id_autuacao,
    m.n_infracoes_total,
    m.valor_total_infracoes,
    m.n_veiculos_com_infracao,
    v.n_infracoes_veiculo_principal,
    v.valor_total_veiculo_principal,
    m.todos_id_auto_infracao,
    m.primeira_data_autuacao,
    m.ultima_data_autuacao,
    COALESCE(a.n_atribuicoes, 0) AS n_atribuicoes_anteriores
FROM multas_por_cpf m
INNER JOIN veiculo_principal v USING (cpf)
INNER JOIN rmi r USING (cpf)
LEFT JOIN veiculo_recente vr USING (cpf)
LEFT JOIN (
    SELECT id_devedor AS cpf, COUNT(*) AS n_atribuicoes
    FROM `{tabela_atribuicao_placeholder}`
    WHERE data_selecao < hoje
    GROUP BY id_devedor
) a USING (cpf)
WHERE NULLIF(TRIM(r.nome_completo), '') IS NOT NULL
    AND NULLIF(TRIM(r.primeiro_nome), '') IS NOT NULL
    AND r.celular_disparo IS NOT NULL
    AND v.n_infracoes_veiculo_principal <= max_infracoes_veiculo
    AND NOT (
        r.sexo = 'masculino'
        AND REGEXP_CONTAINS(
            NORMALIZE_AND_CASEFOLD(COALESCE(vr.tipo_veiculo_recente, ''), NFKD),
            r'motocicleta|motoneta|ciclomotor|triciclo|quadriciclo'
        )
    )
    -- quem foi sorteado há menos de dias_quarentena dias, de qualquer grupo, espera;
    -- no dia D + dias_quarentena (o mesmo dia em que o controle sai da quarentena)
    -- volta para a fila e é sorteado de novo, numa coorte nova
    AND m.cpf NOT IN (
        SELECT id_devedor
        FROM `{tabela_atribuicao_placeholder}`
        WHERE data_selecao > DATE_SUB(hoje, INTERVAL dias_quarentena DAY)
    );


-- ============================================================
-- 7) Seleção (2 × cota) e divisão 50/50, inserida só se a coorte do dia não existe
-- ============================================================
INSERT INTO `{tabela_atribuicao_placeholder}`
WITH selecionados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY FARM_FINGERPRINT(CONCAT(cpf, '|selecao|', CAST(hoje AS STRING))), cpf
        ) AS ordem_selecao
    FROM base_do_dia
    QUALIFY ordem_selecao <= 2 * cota_diaria
),

divididos AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY FARM_FINGERPRINT(CONCAT(cpf, '|grupo|', CAST(hoje AS STRING))), cpf
        ) AS ordem_grupo
    FROM selecionados
)

SELECT
    cpf AS id_devedor,
    hoje AS data_selecao,
    IF(MOD(ordem_grupo, 2) = 0, 'controle', 'tratamento') AS grupo,
    IF(MOD(ordem_grupo, 2) = 0, DATE_ADD(hoje, INTERVAL dias_quarentena DAY), NULL) AS fim_quarentena,
    ordem_selecao,
    ordem_grupo,
    celular_disparo,
    nome_proprietario,
    sexo,
    idade,
    faixa_idade,
    bairro,
    qualidade_tel,
    estrategia_envio,
    tipo_veiculo_recente,
    descricao_veiculo,
    placa_veiculo,
    id_autuacao,
    n_infracoes_total,
    valor_total_infracoes,
    n_veiculos_com_infracao,
    n_infracoes_veiculo_principal,
    valor_total_veiculo_principal,
    todos_id_auto_infracao,
    primeira_data_autuacao,
    ultima_data_autuacao,
    n_atribuicoes_anteriores,
    CURRENT_TIMESTAMP() AS inserido_em
FROM divididos
WHERE dia_util
    AND NOT EXISTS (
        SELECT 1
        FROM `{tabela_atribuicao_placeholder}`
        WHERE data_selecao = hoje
    );


-- ============================================================
-- 8) Lista de disparo: só o tratamento do dia, lido da tabela
-- ============================================================
SELECT
    celular_disparo AS telefone,
    id_devedor AS SubscriberKey,
    `rj-crm-registry.udf.FORMAT_NAME`(nome_proprietario, FALSE) AS nome_proprietario,
    descricao_veiculo,
    placa_veiculo,
    id_autuacao
FROM `{tabela_atribuicao_placeholder}`
WHERE data_selecao = hoje
    AND grupo = 'tratamento'
    AND dia_util
ORDER BY ordem_grupo;
