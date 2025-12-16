

WITH stu AS (
  SELECT
      perm_autor AS id_consorcio,
      cnpj,
      processo,
      data_registro,
      razao_social,
      CASE
        
          WHEN perm_autor = '221000014' THEN '6'
        
          WHEN perm_autor = '221000023' THEN '4'
        
          WHEN perm_autor = '221000032' THEN '3'
        
          WHEN perm_autor = '221000041' THEN '5'
        
          WHEN perm_autor = '221000050' THEN NULL
        
          WHEN perm_autor = '229000010' THEN '1'
        
      END AS cd_consorcio_jae
  FROM
    `rj-smtr`.`br_rj_riodejaneiro_stu`.`operadora_empresa` AS stu
  WHERE
    perm_autor IN ('221000014', '221000023', '221000032', '221000041', '221000050', '229000010')
), consorcio AS (
  SELECT
    COALESCE(s.id_consorcio, j.cd_consorcio) AS id_consorcio,
    CASE
      WHEN s.id_consorcio = '221000050' THEN "Consórcio BRT"
      ELSE j.nm_consorcio
    END AS consorcio,
    s.cnpj,
    s.razao_social,
    s.id_consorcio AS id_consorcio_stu,
    j.cd_consorcio AS id_consorcio_jae,
    s.processo AS id_processo
  FROM `rj-smtr`.`br_rj_riodejaneiro_bilhetagem_staging`.`consorcio` AS j
  FULL OUTER JOIN
    stu AS s
  ON
    j.cd_consorcio = s.cd_consorcio_jae
)
SELECT
  c.id_consorcio,
  c.consorcio,
  m.modo,
  c.cnpj,
  c.razao_social,
  c.id_consorcio_stu,
  c.id_consorcio_jae,
  c.id_processo
FROM consorcio c
LEFT JOIN
  `rj-smtr`.`cadastro_staging`.`consorcio_modo` AS cm
USING (id_consorcio)
LEFT JOIN
  `rj-smtr`.`cadastro`.`modos` AS m
ON
  m.id_modo = cm.id_modo
  AND cm.fonte_id_modo = m.fonte