-- Passos do agente de IA em que alguma URL foi redigida, na janela [$start_datetime, $end_datetime).
-- Intervalo semiaberto: o fim de uma janela é o início da próxima, sem lacuna nem sobreposição.
-- inicio_datahora é DATETIME em hora-parede de São Paulo, por isso a comparação usa DATETIME()
-- e a janela precisa ser calculada em America/Sao_Paulo.
-- passo_atributos_json é do tipo JSON: o acesso por campo dispensa JSONPath e evita o escape $$.
-- O alias só pode ser filtrado fora da CTE: no SQL padrão o WHERE é avaliado antes do SELECT.
-- Sem filtro de partição: o recorte da varredura é feito apenas por inicio_datahora.
WITH passos AS (
    SELECT
        id_passo,
        inicio_datahora,
        ingestao_datahora,
        passo_atributos_json.redacted_urls AS redacted_urls
    FROM `rj-crm-registry.brutos_salesforce.ai_agent_interaction_step`
    WHERE saida_valor_texto LIKE '%URL_Redacted%'
      AND inicio_datahora >= DATETIME('$start_datetime')
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
