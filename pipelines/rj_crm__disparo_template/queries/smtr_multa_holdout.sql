-- ============================================================
-- Holdout - Multas vencidas / não pagas (SMTR)
--
-- Todo dia útil:
--   1) monta a base do dia e tira quem foi sorteado há menos de 40 dias, de qualquer
--      grupo (janela do desfecho de 30 dias + 10 de folga); no dia D+40 a pessoa volta
--      para a fila e pode ser sorteada de novo, numa coorte nova, em qualquer grupo;
--   2) seleciona 2 × cota_diaria em ordem aleatória (FARM_FINGERPRINT(cpf || data));
--      a cota começa em cota_inicial e sobe incremento_cota por coorte até cota_maxima;
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
-- cota do dia = tratamento do dia (seleciona 2×, metade vai pro controle); começa em
-- cota_inicial e sobe incremento_cota a cada coorte já sorteada, até cota_maxima
DECLARE cota_inicial INT64 DEFAULT CAST({cota_inicial_placeholder} AS INT64);
DECLARE incremento_cota INT64 DEFAULT CAST({incremento_cota_placeholder} AS INT64);
DECLARE cota_maxima INT64 DEFAULT CAST({cota_maxima_placeholder} AS INT64);
DECLARE cota_diaria INT64;
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
    -- histórico de multas no dia do sorteio (bloco 3)
    n_multas INT64,
    n_pagas_antes INT64,
    share_pagas_antes FLOAT64,
    pagas_antes STRING,
    pagou_ultimos_12m BOOL,
    dias_medio_ate_pagar FLOAT64,
    dias_desde_ultimo_pagamento INT64,
    dias_desde_primeira_multa INT64,
    dias_desde_ultima_multa INT64,
    n_multas_12m INT64,
    n_abertas INT64,
    valor_aberto NUMERIC,
    valor_maior_multa_aberta NUMERIC,
    dias_multa_aberta_mais_antiga INT64,
    dias_medio_multas_abertas FLOAT64,
    n_abertas_ainda_recorriveis INT64,
    pontos_abertos INT64,
    pontos_12m INT64,
    ja_contestou BOOL,
    ja_indicou_condutor BOOL,
    aderiu_sne BOOL,
    especie_veiculo_recente STRING,
    descricao_veiculo_recente STRING,
    ano_fabricacao_veiculo_recente INT64,
    uf_proprietario_recente STRING,
    municipio_possuidor_recente STRING,
    n_veiculos_historico INT64,
    gravidade_mais_comum_aberta STRING,
    tipificacao_mais_comum_aberta STRING,
    autuador_mais_comum_aberta STRING,
    inserido_em TIMESTAMP
)
PARTITION BY data_selecao
CLUSTER BY grupo, id_devedor;


-- coortes anteriores a hoje: um dia sem execução (feriado, falha) não pula degrau
SET cota_diaria = LEAST(
    cota_maxima,
    cota_inicial + incremento_cota * (
        SELECT COUNT(DISTINCT data_selecao)
        FROM `{tabela_atribuicao_placeholder}`
        WHERE data_selecao < hoje
    )
);


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
    FROM `rj-rmi.rmi_dados_mestres.pessoa_fisica` pf
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
-- 3) Histórico de multas do CPF no dia do sorteio, entre todas as multas
--    (pagas ou não), igual a atributos_por_cpf.sql da análise
--    Tudo é medido antes do disparo, para valer igual para tratamento e controle.
--    "Paga antes" = data_pagamento < hoje; "aberta" = não paga antes.
--    tipo_veiculo_recente é o que decide a exclusão de homens com moto.
-- ============================================================
CREATE TEMP TABLE historico_multas AS
WITH todas_autuacoes AS (
    SELECT *
    FROM (
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
            CAST(id_auto_infracao AS STRING) AS id_auto_infracao,
            data AS data_autuacao,
            datetime_ultima_atualizacao,
            SAFE_CAST(valor_infracao AS NUMERIC) AS valor_infracao,
            SAFE_CAST(pontuacao AS INT64) AS pontuacao,
            data_pagamento,
            data_limite_recurso,
            processo_defesa_autuacao,
            recurso_penalidade_multa,
            processo_troca_real_infrator,
            status_sne,
            gravidade,
            tipificacao_resumida,
            descricao_autuador,
            tipo_veiculo,
            especie_veiculo,
            descricao_veiculo,
            ano_fabricacao_veiculo,
            placa_veiculo,
            uf_proprietario,
            municipio_possuidor_veiculo
        FROM `rj-smtr.transito.autuacao`
        WHERE data BETWEEN DATE '2023-01-01' AND hoje
    )
    WHERE cpf IN (SELECT DISTINCT cpf FROM multas_detalhadas)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cpf, id_auto_infracao
        ORDER BY datetime_ultima_atualizacao DESC
    ) = 1
),

marcadas AS (
    SELECT
        *,
        data_pagamento IS NOT NULL AND data_pagamento < hoje AS paga_antes,
        data_pagamento IS NULL OR data_pagamento >= hoje AS aberta,
        data_autuacao >= DATE_SUB(hoje, INTERVAL 12 MONTH) AS ultimos_12m
    FROM todas_autuacoes
),

-- valor mais comum entre as multas abertas (gravidade, tipificação e autuador)
modas AS (
    SELECT cpf, atributo, valor
    FROM marcadas
    UNPIVOT (valor FOR atributo IN (gravidade, tipificacao_resumida, descricao_autuador))
    WHERE aberta
    GROUP BY cpf, atributo, valor
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf, atributo ORDER BY COUNT(*) DESC, valor) = 1
),

modas_por_cpf AS (
    SELECT
        cpf,
        MAX(IF(atributo = "gravidade", valor, NULL)) AS gravidade_mais_comum_aberta,
        MAX(IF(atributo = "tipificacao_resumida", valor, NULL)) AS tipificacao_mais_comum_aberta,
        MAX(IF(atributo = "descricao_autuador", valor, NULL)) AS autuador_mais_comum_aberta
    FROM modas
    GROUP BY cpf
),

por_cpf AS (
    SELECT
        cpf,

        -- histórico de pagamento
        COUNT(*) AS n_multas,
        COUNTIF(paga_antes) AS n_pagas_antes,
        SAFE_DIVIDE(COUNTIF(paga_antes), COUNT(*)) AS share_pagas_antes,
        CASE
            WHEN COUNTIF(paga_antes) > 0 THEN "já pagou antes"
            WHEN COUNT(*) = 1 THEN "primeira multa"
            ELSE "nunca pagou"
        END AS pagas_antes,
        LOGICAL_OR(paga_antes AND data_pagamento >= DATE_SUB(hoje, INTERVAL 12 MONTH)) AS pagou_ultimos_12m,
        AVG(IF(paga_antes, DATE_DIFF(data_pagamento, data_autuacao, DAY), NULL)) AS dias_medio_ate_pagar,
        DATE_DIFF(hoje, MAX(IF(paga_antes, data_pagamento, NULL)), DAY) AS dias_desde_ultimo_pagamento,
        DATE_DIFF(hoje, MIN(data_autuacao), DAY) AS dias_desde_primeira_multa,
        DATE_DIFF(hoje, MAX(data_autuacao), DAY) AS dias_desde_ultima_multa,
        COUNTIF(ultimos_12m) AS n_multas_12m,

        -- dívida em aberto
        COUNTIF(aberta) AS n_abertas,
        SUM(IF(aberta, valor_infracao, 0)) AS valor_aberto,
        MAX(IF(aberta, valor_infracao, NULL)) AS valor_maior_multa_aberta,
        MAX(IF(aberta, DATE_DIFF(hoje, data_autuacao, DAY), NULL)) AS dias_multa_aberta_mais_antiga,
        AVG(IF(aberta, DATE_DIFF(hoje, data_autuacao, DAY), NULL)) AS dias_medio_multas_abertas,
        COUNTIF(aberta AND data_limite_recurso >= hoje) AS n_abertas_ainda_recorriveis,

        -- pontos na CNH (suspensão a partir de 20/30/40 pontos em 12 meses)
        SUM(IF(aberta, pontuacao, 0)) AS pontos_abertos,
        SUM(IF(ultimos_12m, pontuacao, 0)) AS pontos_12m,

        -- relação com a multa
        LOGICAL_OR(processo_defesa_autuacao IS NOT NULL OR recurso_penalidade_multa IS NOT NULL) AS ja_contestou,
        LOGICAL_OR(processo_troca_real_infrator IS NOT NULL) AS ja_indicou_condutor,
        LOGICAL_OR(status_sne) AS aderiu_sne,

        -- veículo (o da multa mais recente) e quantidade de veículos
        ARRAY_AGG(tipo_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS tipo_veiculo_recente,
        ARRAY_AGG(especie_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS especie_veiculo_recente,
        ARRAY_AGG(descricao_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS descricao_veiculo_recente,
        SAFE_CAST(
            ARRAY_AGG(ano_fabricacao_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS INT64
        ) AS ano_fabricacao_veiculo_recente,
        ARRAY_AGG(uf_proprietario IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS uf_proprietario_recente,
        ARRAY_AGG(municipio_possuidor_veiculo IGNORE NULLS ORDER BY data_autuacao DESC LIMIT 1)[SAFE_OFFSET(0)] AS municipio_possuidor_recente,
        COUNT(DISTINCT placa_veiculo) AS n_veiculos_historico
    FROM marcadas
    GROUP BY cpf
)

SELECT *
FROM por_cpf
LEFT JOIN modas_por_cpf USING (cpf);


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
    h.tipo_veiculo_recente,
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
    COALESCE(a.n_atribuicoes, 0) AS n_atribuicoes_anteriores,
    h.n_multas,
    h.n_pagas_antes,
    h.share_pagas_antes,
    h.pagas_antes,
    h.pagou_ultimos_12m,
    h.dias_medio_ate_pagar,
    h.dias_desde_ultimo_pagamento,
    h.dias_desde_primeira_multa,
    h.dias_desde_ultima_multa,
    h.n_multas_12m,
    h.n_abertas,
    h.valor_aberto,
    h.valor_maior_multa_aberta,
    h.dias_multa_aberta_mais_antiga,
    h.dias_medio_multas_abertas,
    h.n_abertas_ainda_recorriveis,
    h.pontos_abertos,
    h.pontos_12m,
    h.ja_contestou,
    h.ja_indicou_condutor,
    h.aderiu_sne,
    h.especie_veiculo_recente,
    h.descricao_veiculo_recente,
    h.ano_fabricacao_veiculo_recente,
    h.uf_proprietario_recente,
    h.municipio_possuidor_recente,
    h.n_veiculos_historico,
    h.gravidade_mais_comum_aberta,
    h.tipificacao_mais_comum_aberta,
    h.autuador_mais_comum_aberta
FROM multas_por_cpf m
INNER JOIN veiculo_principal v USING (cpf)
INNER JOIN rmi r USING (cpf)
LEFT JOIN historico_multas h USING (cpf)
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
            NORMALIZE_AND_CASEFOLD(COALESCE(h.tipo_veiculo_recente, ''), NFKD),
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
    n_multas,
    n_pagas_antes,
    share_pagas_antes,
    pagas_antes,
    pagou_ultimos_12m,
    dias_medio_ate_pagar,
    dias_desde_ultimo_pagamento,
    dias_desde_primeira_multa,
    dias_desde_ultima_multa,
    n_multas_12m,
    n_abertas,
    valor_aberto,
    valor_maior_multa_aberta,
    dias_multa_aberta_mais_antiga,
    dias_medio_multas_abertas,
    n_abertas_ainda_recorriveis,
    pontos_abertos,
    pontos_12m,
    ja_contestou,
    ja_indicou_condutor,
    aderiu_sne,
    especie_veiculo_recente,
    descricao_veiculo_recente,
    ano_fabricacao_veiculo_recente,
    uf_proprietario_recente,
    municipio_possuidor_recente,
    n_veiculos_historico,
    gravidade_mais_comum_aberta,
    tipificacao_mais_comum_aberta,
    autuador_mais_comum_aberta,
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
