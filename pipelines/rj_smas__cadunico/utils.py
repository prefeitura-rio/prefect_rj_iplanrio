# -*- coding: utf-8 -*-
# ruff: noqa: DTZ007

from datetime import datetime

from google.cloud.storage.blob import Blob


def parse_partition(blob: Blob) -> str:
    name_parts = blob.name.split(".")
    for name_part in name_parts:
        if name_part.startswith("A"):
            partition_info = name_part.replace("A", "")
            break
    parsed_date = datetime.strptime(partition_info, "%y%m%d").strftime("%Y-%m-%d")
    return str(parsed_date)


def parse_txt_first_line(filepath):
    with open(filepath) as f:  # noqa
        first_line = f.readline()
    txt_layout_version = first_line[69:74].strip().replace(".", "")
    dta_extracao_dados_hdr = first_line[82:90].strip()
    txt_date = datetime.strptime(dta_extracao_dados_hdr, "%d%m%Y").strftime("%Y-%m-%d")
    return txt_layout_version, txt_date
