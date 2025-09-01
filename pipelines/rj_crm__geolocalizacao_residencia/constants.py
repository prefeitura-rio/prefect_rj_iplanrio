# -*- coding: utf-8 -*-
"""
Constants for geolocation pipeline
"""

from enum import Enum


class GeolocalizacaoConstants(Enum):
    """Constants for geolocation pipeline"""

    # Dataset and table configuration
    DATASET_ID = "brutos_dados_enriquecidos"
    TABLE_ID = "enderecos_geolocalizados"
    BILLING_PROJECT_ID = "rj-crm-registry"
    BUCKET_NAME = "rj-sms"
    BIGLAKE_TABLE = False

    # File and data configuration
    FILE_FOLDER = "pipelines/data"
    FILE_FORMAT = "parquet"
    SOURCE_FORMAT = "parquet"
    DUMP_MODE = "append"

    # Address column name
    ADDRESS_COLUMN = "endereco_completo"
    RETURN_ORIGINAL_COLS = True

    # Geocoding configuration
    MAX_CONCURRENT_NOMINATIM = 100
    SLEEP_TIME = 1.1
    USE_EXPONENTIAL_BACKOFF = True

    # Provider strategies
    STRATEGY_NOMINATIM = "nominatim"
    STRATEGY_FALLBACK = "fallback"
    STRATEGY_GEOAPIFY_BATCH = "geoapify_batch"

    # Default strategy
    DEFAULT_STRATEGY = "fallback"

    # Geoapify batch configuration
    GEOAPIFY_BATCH_SIZE = 100
    GEOAPIFY_MAX_WAIT_TIME = 300
    GEOAPIFY_POLL_INTERVAL = 5

    # BigQuery address extraction query
    ADDRESS_QUERY = """
WITH enderecos_rmi AS (
  SELECT
    DISTINCT
    LOWER(REGEXP_REPLACE(endereco.principal.tipo_logradouro, r'[^a-zA-Z0-9 ]', '')) AS tipo_logradouro,
    LOWER(REGEXP_REPLACE(endereco.principal.logradouro, r'[^a-zA-Z0-9 ]', '')) AS logradouro_tratado,
    LOWER(REGEXP_REPLACE(endereco.principal.numero, r'[^a-zA-Z0-9 ]', '')) AS numero_porta,
    IFNULL(LOWER(REGEXP_REPLACE(endereco.principal.bairro, r'[^a-zA-Z0-9 ]', '')), "") AS bairro
  FROM `rj-crm-registry.crm_dados_mestres.pessoa_fisica`
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
LIMIT 500
    """
