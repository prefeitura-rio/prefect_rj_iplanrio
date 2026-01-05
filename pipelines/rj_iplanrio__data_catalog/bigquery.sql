DECLARE projetos ARRAY<STRING> DEFAULT ['rj-iplanrio'];
DECLARE sql STRING;

SET sql = (
  SELECT STRING_AGG(sub_sql, ' UNION ALL ')
  FROM (
    SELECT
      FORMAT("""
        SELECT
          '%s' AS projeto,
          '%s' AS dataset,
          t.table_name AS tabela,
          t.table_type,
          DATETIME(t.creation_time) AS criado_em,
          cf.column_name AS coluna_raiz,
          cf.field_path AS coluna,
          cf.data_type AS tipo_dado,
          cf.description AS descricao_coluna,
          opt.descricao_tabela
        FROM
          `%s.%s.INFORMATION_SCHEMA.TABLES` AS t
        JOIN
          `%s.%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS cf
        USING (table_name)
        LEFT JOIN (
          SELECT
            table_name,
            CAST(option_value AS STRING) AS descricao_tabela
          FROM
            `%s.%s.INFORMATION_SCHEMA.TABLE_OPTIONS`
          WHERE option_name = 'description'
        ) AS opt
        USING (table_name)
      """,
      projeto, schema_name,
      projeto, schema_name,
      projeto, schema_name,
      projeto, schema_name
      ) AS sub_sql
    FROM UNNEST(projetos) AS projeto
    JOIN `rj-iplanrio.INFORMATION_SCHEMA.SCHEMATA`
    ON TRUE
  )
);

EXECUTE IMMEDIATE sql;