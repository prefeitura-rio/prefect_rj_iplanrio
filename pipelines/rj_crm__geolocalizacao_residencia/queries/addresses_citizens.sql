WITH enderecos_rmi AS (
  SELECT
    DISTINCT
    LOWER(REGEXP_REPLACE(endereco.principal.tipo_logradouro, r'[^a-zA-Z0-9 ]', '')) AS tipo_logradouro,
    LOWER(REGEXP_REPLACE(endereco.principal.logradouro, r'[^a-zA-Z0-9 ]', '')) AS logradouro_tratado,
    LOWER(REGEXP_REPLACE(endereco.principal.numero, r'[^a-zA-Z0-9 ]', '')) AS numero_porta,
    IFNULL(LOWER(REGEXP_REPLACE(endereco.principal.bairro, r'[^a-zA-Z0-9 ]', '')), "") AS bairro
  FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica`
  WHERE endereco.indicador IS TRUE
    AND (endereco.principal.municipio = "Rio de Janeiro" OR endereco.principal.municipio IS NULL)
    AND endereco.principal.logradouro IS NOT NULL
    AND obito.indicador = FALSE
    AND menor_idade = FALSE
),

enderecos_geolocalizados AS (
  SELECT
    DISTINCT logradouro_tratado, numero_porta, IFNULL(bairro, "") AS bairro
  FROM `rj-crm-registry.brutos_dados_enriquecidos.enderecos_geolocalizados`
)
SELECT
  DISTINCT
    logradouro_tratado,
        enderecos_rmi.numero_porta,
        enderecos_rmi.bairro AS bairro,
    CONCAT(
        CASE WHEN enderecos_rmi.tipo_logradouro IS NULL THEN '' ELSE CONCAT(enderecos_rmi.tipo_logradouro, " ") END,
        CASE WHEN enderecos_rmi.logradouro_tratado IS NULL THEN '' ELSE CONCAT(enderecos_rmi.logradouro_tratado, " ") END,
        CASE WHEN enderecos_rmi.numero_porta IS NULL THEN ',' ELSE CONCAT(enderecos_rmi.numero_porta, ', ') END,
        CASE WHEN enderecos_rmi.bairro IS NULL THEN '' ELSE enderecos_rmi.bairro END,
        ", Rio de Janeiro, RJ, Brasil"
    ) AS endereco_completo
FROM enderecos_rmi
LEFT JOIN enderecos_geolocalizados USING(logradouro_tratado, numero_porta, bairro)
WHERE enderecos_geolocalizados.logradouro_tratado IS NULL
ORDER BY RAND()
LIMIT 10000
