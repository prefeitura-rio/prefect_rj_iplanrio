SELECT
    m.*
FROM
    `rj-crm-registry.ab_test.smtr_multa` AS m
WHERE
    NOT EXISTS (
        SELECT 1
        FROM `rj-crm-registry.brutos_salesforce.status_disparo` AS s
        WHERE
            s.nome_hsm = 'smtrmultasdetransitov1'
            AND s.cpf = m.SubscriberKey
            AND envio_datahora >= DATE_SUB(current_date(), interval 100 day)
    )
LIMIT 100