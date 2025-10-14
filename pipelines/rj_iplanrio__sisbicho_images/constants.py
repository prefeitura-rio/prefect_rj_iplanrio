# -*- coding: utf-8 -*-

from enum import Enum


class SisbichoImagesConstants(Enum):
    """Valores padrão utilizados pelo fluxo rj_iplanrio__sisbicho_images."""

    # Fonte dos dados no BigQuery
    SOURCE_PROJECT = "rj-iplanrio"
    SOURCE_DATASET = "brutos_sisbicho"
    SOURCE_TABLE = "animal"

    # Destino dos dados enxutos (payload do QRCode + URL da foto)
    TARGET_DATASET = "brutos_sisbicho"
    TARGET_TABLE = "animal_imagem"
    DUMP_MODE = "append"

    # Configuração de arquivos temporários para upload ao GCS/BQ
    PARTITION_COLUMN = "ingestao_data"
    FILE_FORMAT = "csv"
    ROOT_FOLDER = "./data_sisbicho_images/"

    # Bucket padrão usado tanto para credenciais quanto para armazenar as imagens
    CREDENTIAL_BUCKET = "rj-iplanrio"
    IMAGE_BUCKET = "rj-iplanrio"
    IMAGE_PREFIX = "raw/sisbicho/fotos"
    STORAGE_PROJECT = "rj-iplanrio"
    CPF_FILTER = "56398972287"

    # Projeto de faturamento do BigQuery / Storage
    BILLING_PROJECT = "rj-iplanrio"

    # Controle de materialização pós dump (mantido como padrão False)
    MATERIALIZE_AFTER_DUMP = False
