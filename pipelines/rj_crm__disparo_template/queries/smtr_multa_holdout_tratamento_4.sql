-- ============================================================
-- Lista de disparo: só o tratamento_4 do dia, lido da tabela
-- A seleção e inserção dos 5 grupos é feita em smtr_multa_holdout.sql
-- ============================================================

DECLARE hoje DATE DEFAULT CURRENT_DATE("America/Sao_Paulo");
DECLARE dia_util BOOL DEFAULT EXTRACT(DAYOFWEEK FROM hoje) NOT IN (1, 7);

SELECT
    celular_disparo AS telefone,
    id_devedor AS SubscriberKey,
    `rj-crm-registry.udf.FORMAT_NAME`(nome_proprietario, FALSE) AS nome_proprietario,
    descricao_veiculo,
    placa_veiculo,
    id_autuacao
FROM `{tabela_atribuicao_placeholder}`
WHERE data_selecao = hoje
    AND grupo = 'tratamento_4'
    AND dia_util
ORDER BY ordem_grupo;
