-- O limite cresce 100 por dia útil a partir da data de início da campanha (start_date_placeholder).
SELECT * EXCEPT (rn)
FROM (
    SELECT
        CONCAT(
            INITCAP(SPLIT(nome_proprietario, ' ')[OFFSET(0)]),
            ' ',
            INITCAP(SPLIT(nome_proprietario, ' ')[OFFSET(ARRAY_LENGTH(SPLIT(nome_proprietario,' ')) - 1)])
        ) AS nome_proprietario,
        * EXCEPT (nome_proprietario),
        ROW_NUMBER() OVER () AS rn
    FROM
        `rj-crm-registry.ab_test.smtr_multa` AS m
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM `rj-crm-registry.brutos_salesforce.status_disparo` AS s
            WHERE
                s.nome_hsm = 'smtrmultasdetransitov1'
                AND s.cpf = m.SubscriberKey
                AND processado_datahora >= DATE_SUB(current_date(), interval 100 day)
        )
        AND EXTRACT(DAYOFWEEK FROM current_date("America/Sao_Paulo")) NOT IN (1, 7)
)
WHERE rn <= (
    -- dias úteis desde start_date (inclusivo: start_date = dia 1)
    DATE_DIFF(CURRENT_DATE("America/Sao_Paulo"), DATE("2026-08-22"), DAY)
    - DATE_DIFF(CURRENT_DATE("America/Sao_Paulo"), DATE("2026-08-22"), WEEK)        -- remove domingos
    - DATE_DIFF(DATE_ADD(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 DAY), DATE_ADD(DATE("2026-08-22"), INTERVAL 1 DAY), WEEK)  -- remove sábados
    + 1  -- start_date conta como dia 1
) * 100;