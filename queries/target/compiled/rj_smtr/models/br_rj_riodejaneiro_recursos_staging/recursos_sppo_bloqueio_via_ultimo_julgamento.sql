
WITH exploded AS (
  SELECT
    id_recurso,
    datetime_update AS data_julgamento,
    SAFE_CAST(COALESCE(JSON_VALUE(items, '$.value'), JSON_VALUE(items, '$.items[0].customFieldItem')) AS STRING
    ) AS julgamento,
    SAFE_CAST(JSON_EXTRACT(items, '$.customFieldId') AS STRING ) AS field_id
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_recursos_staging`.`staging_recursos_sppo_bloqueio_via`,
    UNNEST(items) items
  WHERE
        DATE(data) BETWEEN DATE("2022-01-01T00:00:00")
        AND DATE("2022-01-01T01:00:00")
),
pivotado AS (
  SELECT * EXCEPT(field_id)
  FROM
    exploded
  WHERE field_id = '111865'

),

  julgamento AS (
    SELECT
      p.id_recurso,
      p.julgamento,
      p.data_julgamento,
      t.julgamento AS ultimo_julgamento
    FROM
      pivotado p
    LEFT JOIN
      `rj-smtr`.`br_rj_riodejaneiro_recursos_staging`.`recursos_sppo_bloqueio_via_ultimo_julgamento` AS t
    USING (id_recurso)
  )


SELECT
  id_recurso,
  data_julgamento,
  julgamento
FROM
(
  SELECT
    ROW_NUMBER() OVER(PARTITION BY j.id_recurso ORDER BY j.data_julgamento DESC) AS rn,
    *
  FROM
    julgamento j
  WHERE
    j.julgamento != j.ultimo_julgamento OR j.ultimo_julgamento IS NULL
)
WHERE rn=1