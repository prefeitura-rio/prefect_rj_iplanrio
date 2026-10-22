WITH get_max_id AS (
    SELECT MAX(CAST(contato.id AS int64)) AS max_id_contato
    FROM `rj-crm-registry-dev.staging__intermediario_rmi_conversas.base_receptivo`
    WHERE contato.telefone IS NULL
),
range_ids AS (
    SELECT CAST(id AS string) AS id
    FROM get_max_id,
        UNNEST(GENERATE_ARRAY(1, max_id_contato)) AS id
)
SELECT range_ids.id AS id_contato
FROM range_ids
LEFT JOIN `rj-crm-registry.brutos_wetalkie_staging.contato` AS contato
    ON range_ids.id = id_contato
WHERE id_contato IS NULL
