# -*- coding: utf-8 -*-
import json
import re
import shutil
import textwrap
from datetime import datetime
from pathlib import Path
from typing import List

import basedosdados as bd
import numpy as np
import pandas as pd
import ruamel.yaml as ryaml
from google.cloud.storage.blob import Blob
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.io import get_root_path
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions
from unidecode import unidecode


def get_tables_names_dict() -> dict:
    table_dict = {
        "00": "controle",
        "01": "identificacao_controle",
        "02": "domicilio",
        "03": "familia",
        "04": "identificacao_primeira_pessoa",
        "05": "documento_pessoa",
        "06": "pessoa_deficiencia",
        "07": "escolaridade",
        "08": "trabalho_remuneracao",
        "09": "contato",
        "11": "seguranca_alimentar",
        "12": "condicao_rua",
        "13": "status_familia",
        "14": "documento_membro",
        "15": "habitacao",
        "16": "transferencia_familia",
        "17": "identificacao_membro",
        "18": "exclusao_servidor",
        "19": "exclusao_membro",
        "20": "representante_legal",
        "21": "renda",
        "98": "prefeitura",
        "99": "registros",
    }
    return table_dict


def parse_partition(blob: Blob) -> str:
    name_parts = blob.name.split(".")
    for name_part in name_parts:
        if name_part.startswith("A"):
            partition_info = name_part.replace("A", "")
            break
    parsed_date = datetime.strptime(partition_info, "%y%m%d").strftime("%Y-%m-%d")
    return str(parsed_date)


def parse_txt_first_line(filepath):
    with open(filepath) as f:
        first_line = f.readline()
    txt_layout_version = first_line[69:74].strip().replace(".", "")
    dta_extracao_dados_hdr = first_line[82:90].strip()
    txt_date = datetime.strptime(dta_extracao_dados_hdr, "%d%m%Y").strftime("%Y-%m-%d")
    return txt_layout_version, txt_date


def get_version(input_string):
    match = re.search(r"\d+\.\d+", input_string)
    if match:
        version = match.group()
    else:
        version = None

    return version


def handle_merged_cells(df):
    df = df.reset_index(drop=True)
    last_index_not_na = 0
    for index, row in df.iterrows():
        if pd.isna(row["reg"]):
            if not pd.isna(row["descricao"]):
                df.loc[last_index_not_na, "descricao"] = (
                    str(df.loc[last_index_not_na, "descricao"])
                    + "    \n"
                    + str(row["descricao"]).strip()
                )
        else:
            last_index_not_na = index
    return df[df["posicao"].notnull()]


def parse_tables_from_xlsx(xlsx_input, csv_output, target_pattern, filter_versions):
    # Carregue o arquivo XLSX
    xls = pd.ExcelFile(xlsx_input)
    df_final = pd.DataFrame()
    table_cols = [
        "reg",
        "campo",
        "arquivo_base_versao_7",
        "posicao",
        "tamanho",
        "tipo",
        "transformacao",
        "descricao",
        "nulos",
        "observacoes",
    ]
    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name, header=None, dtype=str)
        for index, row in df.iterrows():
            for col, value in row.items():  # Change this line
                if isinstance(value, str) and target_pattern in value:
                    ini_row = int(index) + 1
                    end_row = len(df)
                    ini_col = int(col)
                    end_col = int(col) + len(table_cols) - 1

                    table_data = df.iloc[
                        ini_row : end_row + 1, ini_col : end_col + 1  # noqa: E203
                    ].copy()
                    table_data = table_data.reset_index(drop=True)
                    if len(table_data.columns) < len(table_cols):
                        table_data["observacoes"] = np.nan

                    table_data.columns = table_data.iloc[0].str.strip().tolist()
                    table_data.columns = table_cols

                    if str(table_data["observacoes"][0]).lower() == "reg":
                        table_data["observacoes"] = np.nan

                    table_data = table_data.iloc[1:]

                    table_data.insert(0, "version", get_version(value))
                    table_data.insert(0, "table", sheet_name)

                    table_data = handle_merged_cells(table_data)

                    df_final = pd.concat([df_final, table_data])

    found_versions = df_final["version"].unique().tolist()
    found_versions.sort()
    log(f"PARSED VERSIONS: {found_versions}")

    df_final = df_final[df_final["version"].isin(filter_versions)]
    log(f"Filtered versions: {filter_versions}")

    df_final["column"] = df_final["arquivo_base_versao_7"].fillna("sem_nome")
    df_final["column"] = df_final["column"].apply(
        lambda x: unidecode(x)
        .replace("-", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .lower()
        .strip()
    )

    df_final["reg_version"] = df_final["reg"] + "____" + df_final["version"]
    column_names = []
    for table_version in df_final["reg_version"].unique():
        dd = df_final[df_final["reg_version"] == table_version]
        column_counter = {}  # Dicionário para rastrear a contagem de colunas repetidas
        for index, row in dd.iterrows():
            col_name = row["column"]
            if col_name in column_counter:
                column_counter[col_name] += 1
                new_col_name = f"{col_name}_{column_counter[col_name]}"
                column_names.append(new_col_name)
            else:
                column_counter[col_name] = 1
                column_names.append(col_name)

    df_final["column"] = column_names
    df_final = df_final.drop(columns=["reg_version"])
    df_final = df_final.reset_index(drop=True)

    output_filepath = Path(csv_output)
    log(f"Parsed csv file: {output_filepath}")
    df_final.to_csv(output_filepath, index=False)

    return df_final


def get_staging_partitions_versions(project_id, dataset_id, table_id):
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = list(
        st.client["storage_staging"]
        .bucket(project_id)
        .list_blobs(prefix=f"staging/{st.dataset_id}/{st.table_id}/")
    )

    partitions_list = []
    for blob in blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                if key == "versao_layout_particao":
                    partitions_list.append(value)
    return list(set(partitions_list))


def parse_version_from_blob(name):
    version = name.split("BMM_V")[1].split(".")[0].replace("_", "")
    version = f"0{version}" if len(version) == 3 else version
    return version


def download_files_from_storage_raw(
    dataset_id, table_id, staging_partitions_list, output_path
):
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = list(
        st.client["storage_staging"]
        .bucket(st.bucket_name)
        .list_blobs(prefix=f"raw/{st.dataset_id}/{st.table_id}/")
    )

    raw_blobs_files = [
        blob for blob in blobs if "xlsx" in blob.name or "xls" in blob.name
    ]
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    raw_filespaths_to_ingest = []
    raw_versions_to_ingest = []
    for blob in raw_blobs_files:
        version = parse_version_from_blob(name=blob.name)
        if version not in staging_partitions_list:
            download_file_path = output_path / blob.name.split("/")[-1]
            blob.download_to_filename(download_file_path)
            raw_filespaths_to_ingest.append(download_file_path)
            raw_versions_to_ingest.append(version)
            log(f"Downloaded file: {download_file_path}")
    return raw_filespaths_to_ingest


def parse_xlsx_files_and_save_partition(output_path, raw_filespaths_to_ingest):
    shutil.rmtree(output_path, ignore_errors=True)
    for raw_file in raw_filespaths_to_ingest:
        name = str(raw_file).split("/")[-1]
        version = parse_version_from_blob(name=name)
        csv_output = Path(output_path) / f"versao_layout_particao={version}"
        csv_output.mkdir(parents=True, exist_ok=True)
        csv_name = name.replace(".xlsx", ".csv").replace(".xls", ".csv")

        version_float = str(float(version[:2] + "." + version[2:]))
        version_float = (
            version_float if len(version_float) == 4 else f"{version_float}0"
        )

        df_final = parse_tables_from_xlsx(  # noqa
            xlsx_input=raw_file,
            csv_output=csv_output / csv_name,
            target_pattern="LEIAUTE VERSÃO",
            filter_versions=[version_float],
        )

    return output_path


def create_table_and_upload_to_storage(dataset_id, table_id, output_path):
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    log(f"Ingest files from outputpath: {output_path}")

    if not tb.table_exists(mode="staging"):
        log(
            f"Table {dataset_id}.{table_id} does not exist in STAGING, need to create first"
        )
        tb.create(
            path=output_path,
            if_storage_data_exists="replace",
            biglake_table=True,
        )
        log(f"Successfully created table  {dataset_id}.{table_id} in STAGING")
    else:
        log(f"Table  {dataset_id}.{table_id} exists in STAGING, will append data")
        tb.append(filepath=output_path, if_exists="replace")
        log(f"Successfully uploaded data to Storage for table  {dataset_id}.{table_id}")
    return output_path


def get_layout_table_from_staging(
    project_id, dataset_id, table_id, layout_dataset_id, layout_table_id
):
    query = """
        SELECT
            t1.* EXCEPT(column),
            t2.* EXCEPT(descricao)
        FROM `rj-smas.protecao_social_cadunico_staging.layout` t1
        LEFT JOIN `rj-smas.protecao_social_cadunico_staging.layout_dicionario_colunas` t2
        ON t1.column = t2.column
        WHERE table != "REG_99_DIARIA"
    """
    log(f"Using project_id: {project_id}\n\n{query}")
    dataframe = bd.read_sql(query=query, billing_project_id=project_id, from_file=True)
    # get only versions that are in staging data table registro_familia
    versions = get_staging_partitions_versions(
        project_id=project_id, dataset_id=dataset_id, table_id=table_id
    )
    log(
        f"Dataframe will be filtered using versions from {project_id}.{dataset_id}_staging.{table_id}: {versions}"  # noqa
    )

    return dataframe[dataframe["versao_layout_particao"].isin(versions)]


def load_ruamel():
    """
    Loads a YAML file.
    """
    ruamel = ryaml.YAML()
    ruamel.default_flow_style = False
    ruamel.top_level_colon_align = True
    ruamel.indent(mapping=2, sequence=4, offset=2)
    return ruamel


def dump_dict_to_dbt_yaml(schema, schema_yaml_path):
    ruamel = load_ruamel()
    for model in schema["models"]:
        for col in model["columns"]:
            if len(col["description"]) > 1024:
                model_name = model["name"]
                col_name = col["name"]
                col_description_lenght = len(col["description"])
                log_msg = (
                    "Column exced max bigquery description lenght"
                    + f"Model: {model_name}\n"
                    + f"Column: {col_name}\n"
                    + f"Description lenght:{col_description_lenght}"
                )
                log(log_msg, level="warning")
    log(f"Dumping schema to {schema_yaml_path}")
    ruamel.dump(
        schema,
        open(Path(schema_yaml_path), "w", encoding="utf-8"),
    )


def convert_string_to_json(s):
    if s is not None:
        try:
            return json.loads(str(s))
        except Exception as e:
            log(s)
            raise BaseException(e)
    else:
        return s


def create_cadunico_dbt_consolidated_models(
    dataframe: pd.DataFrame, model_dataset_id: str, model_table_id: str
):

    df = dataframe.copy()
    df["reg"] = df["reg"].apply(lambda x: x if len(x) > 1 else f"0{x}")
    df["version"] = (
        df["version"].str.replace(".", "").apply(lambda x: x if len(x) > 3 else f"0{x}")
    )
    df = df.sort_values(["reg", "versao_layout_particao"])
    df["version"] = np.where(
        df["coluna_esta_versao_anterior"] == "False",
        df["versao_layout_anterior"],
        df["version"],
    )
    df["dicionario_atributos"] = df["dicionario_atributos"].apply(
        lambda s: convert_string_to_json(s)
    )

    # remove columns that are empty
    df = df[np.logical_not(df["column"].str.contains("vazio"))]
    df = df.sort_values(["reg", "versao_layout_particao"])
    tables_dict = get_tables_names_dict()
    schema = {"version": 2, "models": []}
    log_created_models = []

    last_version = max(df["version"])
    log(f"Schema.yml will use the last version of the layout: {last_version}")

    for table_number in df["reg"].unique():
        table_schema = {}
        table_model_name = tables_dict[table_number]
        model_name = (
            table_model_name
            if "test" not in model_dataset_id
            else f"{table_model_name}_test"
        )

        tables = df[df["reg"] == table_number]
        versions = tables["version"].unique()
        table_schema["name"] = model_name
        table_schema["description"] = f"Table {model_name} from number {table_number}"
        table_schema["columns"] = []
        final_query = ""
        ini_query = """
            SELECT
        """
        end_query = """
                SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
                SAFE_CAST(data_particao AS DATE) AS data_particao
            FROM `rj-smas.__dataset_id_replacer___staging.__model_table_id_replacer__`
            WHERE versao_layout_particao = '__version_replacer__'
                AND SUBSTRING(text,38,2) = '__table_number_replacer__'

            UNION ALL

        """
        ini_query = textwrap.dedent(ini_query)
        end_query = textwrap.dedent(end_query)

        for version in versions:
            table_version = tables[tables["version"] == version]
            table_version = table_version.sort_values("column")
            columns = table_version["column"].tolist()
            table_name_original = f"{model_name}_{version}"
            table_name = (
                f"{table_name_original}_test"
                if "test" in model_dataset_id
                else table_name_original
            )
            columns = []
            for index, row in table_version.iterrows():
                column = row["column"]
                col_name = f"SUBSTRING(text,{row['posicao']},{row['tamanho']})"
                col_name_padronizado = row["nome_padronizado"]
                bigquery_type = row["bigquery_type"]
                bigquery_type = bigquery_type if bigquery_type is not None else "STRING"
                date_format = row["date_format"]
                ajuste_decimal = row["ajuste_decimal"]
                col_in_last_version = row["coluna_esta_versao_anterior"]

                col_name_padronizado = (
                    col_name_padronizado
                    if col_name_padronizado is not None
                    else col_name
                )
                dicionario_atributos = row["dicionario_atributos"]

                if dicionario_atributos is not None:
                    if "id_" in col_name_padronizado:
                        col_name_padronizado_dict_atr = col_name_padronizado.replace(
                            "id_", "", 1
                        )
                    else:
                        raise Exception(
                            f"col_name_padronizado: {col_name_padronizado} should have id_ in the name"  # noqa
                        )

                if col_in_last_version == "False":
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + f"    NULL AS {col_name_padronizado}, --Essa coluna não esta na versao posterior"  # noqa
                    )
                    if dicionario_atributos is not None:
                        col_expression = (
                            col_expression
                            + f"\n    --column: {column}\n"
                            + f"    NULL AS {col_name_padronizado_dict_atr}, --Essa coluna não esta na versao posterior"  # noqa
                        )
                else:
                    if bigquery_type == "DATE":
                        col_expression = (
                            f"\n    --column: {column}\n"
                            + "    SAFE.PARSE_DATE(\n"
                            + f"        '{date_format}',\n"
                            + "        CASE\n"
                            + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\s*$') THEN NULL\n"  # noqa
                            + f"            ELSE TRIM({col_name})\n"
                            + "        END"
                            + f"    ) AS {col_name_padronizado},"
                        )
                    elif bigquery_type == "INT64":
                        col_expression = (
                            f"\n    --column: {column}\n"
                            + "    SAFE_CAST(\n"
                            + "        CASE\n"
                            + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\s*$') THEN NULL\n"  # noqa
                            + f"            ELSE TRIM({col_name})\n"
                            + f"        END AS {bigquery_type}\n"
                            + f"    ) AS {col_name_padronizado},"
                        )
                    elif bigquery_type == "FLOAT64":
                        col_expression = (
                            f"\n    --column: {column}\n"
                            + "    SAFE_CAST(\n"
                            + "        CASE\n"
                            + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\s*$') THEN NULL\n"  # noqa
                            + f"            ELSE SAFE_CAST( TRIM({col_name}) AS INT64) / {ajuste_decimal}\n"  # noqa
                            + f"        END AS {bigquery_type}\n"
                            + f"    ) AS {col_name_padronizado},"
                        )
                    else:
                        col_expression = (
                            f"\n    --column: {column}\n"
                            + "    CAST(\n"
                            + "        CASE\n"
                            + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\s*$') THEN NULL\n"  # noqa
                            + f"            ELSE TRIM({col_name})\n"
                            + f"        END AS {bigquery_type}\n"
                            + f"    ) AS {col_name_padronizado},"
                        )

                        if dicionario_atributos is not None:
                            col_expression = (
                                col_expression
                                + f"\n    --column: {column}\n"
                                + "    CAST(\n"
                                + "        CASE\n"
                                + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\s*$') THEN NULL\n"  # noqa
                            )
                            for key in dicionario_atributos.keys():
                                col_expression = (
                                    col_expression
                                    + f"            WHEN REGEXP_CONTAINS({col_name}, r'^{key}$') THEN '{dicionario_atributos[key]}'\n"  # noqa
                                )
                            col_expression = (
                                col_expression
                                + f"            ELSE TRIM({col_name})\n"
                                + f"        END AS {bigquery_type}\n"
                                + f"    ) AS {col_name_padronizado_dict_atr},"
                            )

                columns.append(col_expression)

                # get the description from the last version of the layout
                if version == last_version:
                    col_description = (
                        row["descricao"]
                        if row["descricao"] is not None
                        else "Sem descrição"
                    )
                    col_description = (
                        re.sub(r"\s+", " ", col_description)
                        .replace(";", "\n")
                        .replace("\\", "")
                        .replace(". ", "\n")
                        .replace("\n ", "\n")
                        .replace(" - ", "-")
                    )
                    col_description = col_description + f" | version: {version}"
                    # bigquery limits the description to 1024 characters
                    col_description = col_description[:1020]

                    col_schema = {
                        "name": col_name_padronizado,
                        "description": col_description,
                    }
                    table_schema["columns"].append(col_schema)
                    if dicionario_atributos is not None:
                        col_schema_dict_atr = {
                            "name": col_name_padronizado_dict_atr,
                            "description": col_description,
                        }
                        table_schema["columns"].append(col_schema_dict_atr)

            column_dict = {}

            table_query = ini_query + "\n".join(columns) + end_query
            table_query = table_query.replace(
                "__dataset_id_replacer__", model_dataset_id
            )
            table_query = table_query.replace("__table_id_replacer__", table_name)
            table_query = table_query.replace("__table_number_replacer__", table_number)
            table_query = table_query.replace("__version_replacer__", version)
            table_query = table_query.replace(
                "__model_table_id_replacer__", model_table_id
            )
            final_query += table_query
        final_query = final_query.rsplit("UNION ALL", 1)[0]
        for item in table_schema["columns"]:
            column_dict[item["name"]] = item
        table_schema["columns"] = list(column_dict.values())
        schema["models"].append(table_schema)

        root_path = get_root_path()

        model_path = root_path / f"queries/models/{model_dataset_id}"
        sql_filepath = model_path / f"{model_name}.sql"

        sql_filepath.parent.mkdir(parents=True, exist_ok=True)
        log_created_models.append(str(sql_filepath))
        config_partition = """
                {{
                    config(
                        materialized="table",
                        partition_by={
                            "field": "data_particao",
                            "data_type": "date",
                            "granularity": "month",
                        }
                    )
                }}

        """
        final_query = textwrap.dedent(config_partition) + final_query

        with open(sql_filepath, "w") as text_file:
            text_file.write(final_query)

    dump_dict_to_dbt_yaml(schema=schema, schema_yaml_path=model_path / "schema.yml")
    json_log = json.dumps(log_created_models, indent=4)
    log(f"created {len(log_created_models)} prod models : {json_log}")


def columns_version_control_diff(df):
    df_concat = pd.DataFrame()
    for reg in df["reg"].unique().tolist():
        df_table = df[df["reg"] == reg]
        table_versions = df_table["versao_layout_particao"].unique().tolist()
        table_versions = [table_versions[0]] + table_versions

        for i in range(len(table_versions) - 1):
            current_version = table_versions[i]
            df_version = df_table[df_table["versao_layout_particao"] == current_version]
            version_columns = df_version["column"].tolist()

            next_version = table_versions[i + 1]
            df_next_version = df_table[
                df_table["versao_layout_particao"] == next_version
            ]
            next_version_columns = df_next_version["column"].tolist()

            control_column_version = []
            prev_versions = []
            for nv_col in next_version_columns:
                prev_versions.append(current_version)
                if nv_col not in version_columns:
                    control_column_version.append("False")
                    log_msg = (
                        f"Table {reg} version {next_version}\n"
                        + f"Column {nv_col} not found in version {current_version}"
                    )
                    log(log_msg, level="warning")
                else:
                    control_column_version.append("True")

            df_next_version["versao_layout_anterior"] = prev_versions
            df_next_version["coluna_esta_versao_anterior"] = control_column_version

            df_concat = pd.concat([df_concat, df_next_version])
    return df_concat.sort_values(["reg", "versao_layout_particao"])


def parse_columns_version_control(df):
    log("ASCENDING SEARCH\n")
    df_ascending = columns_version_control_diff(
        df=df.sort_values(["reg", "versao_layout_particao"])
    )
    # create new row for versions lass than versao_layout_anterior
    versions = df_ascending["versao_layout_particao"].unique()
    versions.sort()

    df_new = pd.DataFrame()
    diff = df_ascending[df_ascending["coluna_esta_versao_anterior"] == "False"]
    for index, row in diff.iterrows():
        df_new = pd.concat([df_new, row.to_frame().T])
        for version in versions:
            if version < row["versao_layout_anterior"]:
                new_row = row.copy()
                new_row["versao_layout_anterior"] = version
                df_new = pd.concat([df_new, new_row.to_frame().T])

    df_ascending["coluna_esta_versao_anterior"] = "True"
    df_final = pd.concat([df_ascending, df_new])
    df_final = df_final.sort_values(["reg", "versao_layout_particao"])

    log("\nDESCENDING SEARCH\n")
    df_descending = columns_version_control_diff(
        df=df.sort_values(["reg", "versao_layout_particao"], ascending=False)
    )

    # create new row for versions lass than versao_layout_anterior
    versions = df_descending["versao_layout_particao"].unique()
    versions.sort()
    df_new = pd.DataFrame()
    diff = df_descending[df_descending["coluna_esta_versao_anterior"] == "False"]
    for index, row in diff.iterrows():
        df_new = pd.concat([df_new, row.to_frame().T])
        for version in versions:
            if version > row["versao_layout_anterior"]:
                new_row = row.copy()
                new_row["versao_layout_anterior"] = version
                df_new = pd.concat([df_new, new_row.to_frame().T])

    df_final = pd.concat([df_final, df_new])
    df_final = df_final.sort_values(["reg", "versao_layout_particao"])
    return df_final


def create_layout_column_cross_version_control_bq_table(
    dataframe, dataset_id, table_id
):

    new_dataframe = parse_columns_version_control(df=dataframe)

    output_path = Path("/tmp/cadunico/final_layout")
    shutil.rmtree(output_path, ignore_errors=True)
    to_partitions(
        data=new_dataframe,
        partition_columns=["versao_layout_particao"],
        savepath=output_path,
    )
    table_id = table_id + "_columns_version_control"
    create_table_and_upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        biglake_table=True,
    )
    log(f"Table {dataset_id}.{table_id} created in STAGING")

    return new_dataframe


def update_layout_from_storage_and_create_versions_dbt_models(
    project_id: str,
    layout_dataset_id: str,
    layout_table_id: str,
    output_path: str | Path,
    model_dataset_id: str,
    model_table_id: str,
    force_create_models: bool,
):
    staging_partitions_list = get_staging_partitions_versions(
        project_id=project_id, dataset_id=layout_dataset_id, table_id=layout_table_id
    )
    raw_filespaths_to_ingest = download_files_from_storage_raw(
        dataset_id=layout_dataset_id,
        table_id=layout_table_id,
        staging_partitions_list=staging_partitions_list,
        output_path="/tmp/cadunico/raw",
    )

    if raw_filespaths_to_ingest:
        log(f"FILES TO INGEST: {raw_filespaths_to_ingest}")
        output_path = parse_xlsx_files_and_save_partition(
            output_path=output_path,
            raw_filespaths_to_ingest=raw_filespaths_to_ingest,
        )
        output_path = create_table_and_upload_to_storage(
            dataset_id=layout_dataset_id,
            table_id=layout_table_id,
            output_path=output_path,
        )
    else:
        log("NO LAYOUT FILES TO INGEST")

    if raw_filespaths_to_ingest or force_create_models:
        log("GET LAYOUT TABLE FROM STAGING")
        dataframe = get_layout_table_from_staging(
            project_id=project_id,
            dataset_id=model_dataset_id,
            table_id=model_table_id,
            layout_dataset_id=layout_dataset_id,
            layout_table_id=layout_table_id,
        )

        log("CREATE LAYOUT COLUMNS VERSION CONTROL IN BQ")
        df_final = create_layout_column_cross_version_control_bq_table(
            dataframe=dataframe, dataset_id=layout_dataset_id, table_id=layout_table_id
        )

        log("CREATE DBT CONSOLIDATED MODELS")
        create_cadunico_dbt_consolidated_models(
            dataframe=df_final,
            model_dataset_id=model_dataset_id,
            model_table_id=model_table_id,
        )


def get_dbt_models_to_materialize(
    project_id: str,
    dataset_id: str,
    table_id: str,
    layout_dataset_id: str,
    layout_table_id: str,
    layout_output_path: str | Path,
    force_create_models: bool,
    aditional_dbt_models_to_materialize: str,
) -> List[dict]:
    """
    Get tables parameters to materialize from queries/models/{dataset_id}/.

    Args:
        dataset_id (str): The dataset ID.
        ingested_files_output (str | Path): The path to the ingested files.
    """
    # if first_execution:
    tables_dict = get_tables_names_dict()
    aditional_dbt_models_to_materialize = aditional_dbt_models_to_materialize.split(",")
    dbt_models_to_materialize_list = (
        list(tables_dict.values()) + aditional_dbt_models_to_materialize
    )

    log("STARTING LAYOUT TABLE MANAGEMENT AND DBT MODELS CREATION")
    update_layout_from_storage_and_create_versions_dbt_models(
        project_id=project_id,
        layout_dataset_id=layout_dataset_id,
        layout_table_id=layout_table_id,
        output_path=layout_output_path,
        model_dataset_id=dataset_id,
        model_table_id=table_id,
        force_create_models=force_create_models,
    )
    log("FINISHED LAYOUT TABLE MANAGEMENT AND DBT MODELS CREATION")

    log("STARTING GETTING DBT MODELS TO MATERIALIZE")
    dataset_id_original = dataset_id
    root_path = get_root_path()

    # if not only_version_tables:
    parameters_list = []
    # add hamonized tables to materialize
    queries_dir = root_path / f"queries/models/{dataset_id_original}"
    files_path = [str(q) for q in queries_dir.iterdir() if q.is_file()]
    files_path.sort()
    tables = [
        q.replace(".sql", "").split("/")[-1].split("__")[-1]
        for q in files_path
        if q.endswith(".sql")
    ]
    table_dbt_alias = [
        True if "__" in q.split("/")[-1] else False
        for q in files_path
        if q.endswith(".sql")
    ]
    log(f"TABLES TO MATERIALIZE LIST CONTROL: {dbt_models_to_materialize_list}")
    for _table_id_, dbt_alias in zip(tables, table_dbt_alias):
        if _table_id_ in dbt_models_to_materialize_list:
            parameters = {
                "dataset_id": dataset_id_original,
                "table_id": f"{_table_id_}",
                "dbt_alias": dbt_alias,
            }
            parameters_list.append(parameters)

    # reorder tables to materialize to put aditional_dbt_models_to_materialize in the end os the materializations # noqa
    parameters_list_ordered = []
    for model_name in dbt_models_to_materialize_list:
        for model in parameters_list:
            model_table_id = model.get("table_id")
            if model_name == model_table_id:
                log(f"ADDING MODEL TO ORDERED PARAMETERS LIST: {model_name}\n{model}")
                parameters_list_ordered.append(model)
                break

    parameters_list_log = json.dumps(parameters_list_ordered, indent=4)
    log(f"{len(parameters_list_ordered)} TABLES TO MATERIALIZE:\n{parameters_list_log}")
    return parameters_list_ordered


def update_local_layout():
    project_id = "rj-smas"
    layout_dataset_id = "protecao_social_cadunico"
    layout_table_id = "layout"

    model_dataset_id = "protecao_social_cadunico"
    model_table_id = "registro_familia"
    force_create_models = True

    output_path = f"/tmp/cadunico/layout_parsed/{layout_dataset_id}/{layout_table_id}"

    update_layout_from_storage_and_create_versions_dbt_models(
        project_id=project_id,
        layout_dataset_id=layout_dataset_id,
        layout_table_id=layout_table_id,
        output_path=output_path,
        model_dataset_id=model_dataset_id,
        model_table_id=model_table_id,
        force_create_models=force_create_models,
    )
