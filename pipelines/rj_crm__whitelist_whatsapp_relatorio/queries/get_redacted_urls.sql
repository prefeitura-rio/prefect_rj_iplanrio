-- Passos do agente de IA em que alguma URL foi redigida, na janela [$start_datetime, $end_datetime).
-- Fonte em `$project.$dataset_id.$table_id`: vem dos replacements do deployment (§7.5 do
-- styleguide), não de literal no SQL, para que o arquivo descreva o recorte e não o ambiente.
-- Intervalo semiaberto: o fim de uma janela é o início da próxima, sem lacuna nem sobreposição.
-- inicio_datahora é DATETIME em hora-parede de São Paulo, por isso a comparação usa DATETIME()
-- e a janela precisa ser calculada em America/Sao_Paulo.
-- passo_atributos_json é do tipo JSON: o acesso por campo dispensa JSONPath e evita o escape $$.
-- O critério é só a chave redacted_urls. Até 2026-09-07 toda linha com a chave também trazia
-- o marcador URL_Redacted em saida_valor_texto; a partir de 2026-09-08 a ingestão passou a
-- gravar saida_valor_texto como NULL nesses passos, e filtrar pelo marcador zerava o relatório.
-- O alias só pode ser filtrado fora da CTE: no SQL padrão o WHERE é avaliado antes do SELECT.
-- Sem filtro de partição: o recorte da varredura é feito apenas por inicio_datahora.
WITH passos AS (
    SELECT
        id_passo,
        inicio_datahora,
        ingestao_datahora,
        passo_atributos_json.redacted_urls AS redacted_urls
    FROM `$project.$dataset_id.$table_id`
    WHERE inicio_datahora >= DATETIME('$start_datetime')
      AND inicio_datahora < DATETIME('$end_datetime')
)
SELECT
    id_passo,
    inicio_datahora,
    ingestao_datahora,
    redacted_urls
FROM passos
WHERE redacted_urls IS NOT NULL
ORDER BY inicio_datahora DESC
