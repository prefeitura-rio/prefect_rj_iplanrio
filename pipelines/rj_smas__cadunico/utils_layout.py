# -*- coding: utf-8 -*-
# ruff: noqa

import json
import re
import shutil
import textwrap
from pathlib import Path
from typing import List

import basedosdados as bd
import numpy as np
import pandas as pd
import ruamel.yaml as ryaml
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.gcs import (
    list_blobs_with_prefix,
)
from iplanrio.pipelines_utils.logging import log
from pipelines.rj_smas__cadunico.utils_logging import PipelineLogger
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


def parse_version_from_blob(name: str):
    version = name.split("BMM_V")[1].split(".")[0].replace("_", "")
    version = f"0{version}" if len(version) == 3 else version
    return version


def get_version(input_string: str):
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
                    str(df.loc[last_index_not_na, "descricao"]) + "    \n" + str(row["descricao"]).strip()
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

                    table_data = df.iloc[ini_row : end_row + 1, ini_col : end_col + 1].copy()
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
        lambda x: unidecode(x).replace("-", "_").replace(" ", "_").replace("/", "_").lower().strip()
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


def parse_xlsx_files_and_save_partition(output_path: str, raw_filespaths_to_ingest: List) -> str:
    shutil.rmtree(output_path, ignore_errors=True)
    for raw_file in raw_filespaths_to_ingest:
        name = str(raw_file).split("/")[-1]
        version = parse_version_from_blob(name=name)
        csv_output = Path(output_path) / f"versao_layout_particao={version}"
        csv_output.mkdir(parents=True, exist_ok=True)
        csv_name = name.replace(".xlsx", ".csv").replace(".xls", ".csv")

        version_float = str(float(version[:2] + "." + version[2:]))
        version_float = version_float if len(version_float) == 4 else f"{version_float}0"

        df_final = parse_tables_from_xlsx(  # noqa
            xlsx_input=raw_file,
            csv_output=csv_output / csv_name,
            target_pattern="LEIAUTE VERSÃO",
            filter_versions=[version_float],
        )

    return str(output_path)


def get_existing_partitions(prefix: str, bucket_name: str, dataset_id: str, table_id: str) -> List[str]:
    """
    Lista as partições já processadas na área de staging.

    Args:
        prefix (str): Prefixo do caminho para listar partições.
        bucket_name (str): Nome do bucket GCS.

    Returns:
        List[str]: Lista de partições no formato `YYYY-MM-DD`.
    """
    # List blobs in staging area

    log(f"Listing blobs in staging area with prefix {bucket_name}/{prefix}")
    log(f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}")

    staging_blobs = list_blobs_with_prefix(bucket_name=bucket_name, prefix=prefix)
    log(f"Found {len(staging_blobs)} blobs in staging area")

    # Extract partition information from blobs
    staging_partitions = []
    for blob in staging_blobs:
        for folder in blob.name.split("/"):
            if "=" in folder:
                key = folder.split("=")[0]
                value = folder.split("=")[1]
                if key == "versao_layout_particao":
                    staging_partitions.append(value)
    staging_partitions = list(set(staging_partitions))
    log(f"Staging partitions {len(staging_partitions)}: {staging_partitions}")
    return staging_partitions


def download_files_from_storage_raw(
    staging_partitions_list: List,
    output_path_str: str,
    raw_bucket: str = "rj-smas",
    raw_prefix_area="raw/protecao_social_cadunico/layout",
):
    raw_blobs = list_blobs_with_prefix(bucket_name=raw_bucket, prefix=raw_prefix_area)
    log(f"https://console.cloud.google.com/storage/browser/{raw_bucket}/{raw_prefix_area}")
    log(f"Found {len(raw_blobs)} blobs in raw area")

    raw_blobs_files = []
    for blob in raw_blobs:
        if blob.name and ("xlsx" in blob.name or "xls" in blob.name):
            raw_blobs_files.append(blob)

    output_path = Path(output_path_str)
    output_path.mkdir(parents=True, exist_ok=True)

    raw_filespaths_to_ingest = []
    raw_versions_to_ingest = []
    raw_versions = []
    for blob in raw_blobs_files:
        version = parse_version_from_blob(name=blob.name)
        raw_versions.append(version)
        if version not in staging_partitions_list:
            download_file_path = output_path / blob.name.split("/")[-1]
            blob.download_to_filename(download_file_path)
            raw_filespaths_to_ingest.append(download_file_path)
            raw_versions_to_ingest.append(version)
            log(f"Downloaded file: {download_file_path}")

    log(f"Raw partitions {len(raw_versions)}: {raw_versions}")

    # 📊 COMPARAÇÃO DETALHADA DE VERSÕES
    all_versions = sorted(set(raw_versions + staging_partitions_list))
    comparison_log = {}

    for version in all_versions:
        comparison_log[version] = {
            "staging": version in staging_partitions_list,
            "raw": version in raw_versions,
        }

    log("📊 COMPARAÇÃO DE VERSÕES (staging vs raw):")
    for version, status in comparison_log.items():
        staging_status = "✅" if status["staging"] else "❌"
        raw_status = "✅" if status["raw"] else "❌"
        action = ""

        if status["raw"] and not status["staging"]:
            action = "→ SERÁ INGERIDO"
        elif status["staging"] and not status["raw"]:
            action = "→ SÓ EM STAGING"
        elif status["staging"] and status["raw"]:
            action = "→ JÁ PROCESSADO"
        else:
            action = "→ INCONSISTENTE"

        log(f"   {version}: staging={staging_status} raw={raw_status} {action}")

    log(
        f"📈 RESUMO: {len(raw_versions)} versões no raw, {len(staging_partitions_list)} no staging, {len(raw_filespaths_to_ingest)} para ingerir"
    )

    return raw_filespaths_to_ingest


def get_layout_table_from_staging(project_id, dataset_id, registo_familia_table_id, layout_table_id):
    log("VALIDATE COLUMN NAMES\n")
    query = f"""
    WITH layout_unique_columns AS (
        SELECT
        column as coluna_layout,
        max(descricao) as descricao
        FROM `{project_id}.{dataset_id}_staging.{layout_table_id}`
        GROUP BY 1
    ),

    sheets_unique_columns AS (
        SELECT
        column as coluna_sheets
        FROM `{project_id}.{dataset_id}_staging.layout_dicionario_colunas`

    )

    SELECT
        t2.coluna_sheets,
        t1.coluna_layout,
        t1.descricao
    FROM layout_unique_columns t1
    FULL OUTER JOIN sheets_unique_columns t2
    ON t1.coluna_layout = t2.coluna_sheets
    WHERE coluna_sheets IS NULL
    ORDER BY coluna_layout
    """

    log(f"Columns Validation Query:\n{query}")
    df_validation = bd.read_sql(query=query, billing_project_id=project_id, from_file=True)

    if df_validation is not None:
        columns_to_create = df_validation.to_dict(orient="records")
        if len(columns_to_create) >= 1:
            columns_to_create_str = json.dumps(columns_to_create, indent=2, ensure_ascii=False)

            raise_msg = ""
            raise_msg += (
                "Antes de prosseguir as colunas abaixo precisam ser criadas na planilha dicionario_colunas_cadunico:\n"
            )
            raise_msg += "https://docs.google.com/spreadsheets/d/1VgtSVom_s4QsJoOws6caPZxGfwYrpjpoSjbOrqhRbM8/edit?gid=542906575#gid=542906575\n\n"
            raise_msg += f"Colunas que devem ser inseridas:\n{columns_to_create_str}"
            raise ValueError(raise_msg)
    log(f"GET CONSOLIDATE LAYOUT TO CREATE: `{project_id}.{dataset_id}_staging.layout_columns_version_control`")

    query = f"""
        SELECT
            t1.* EXCEPT(column),
            t2.* EXCEPT(descricao)
        FROM `{project_id}.{dataset_id}_staging.{layout_table_id}` t1
        LEFT JOIN `{project_id}.{dataset_id}_staging.layout_dicionario_colunas` t2
        ON t1.column = t2.column
        WHERE table != "REG_99_DIARIA"
    """
    log(f"Using project_id: {project_id}\n\n{query}")
    dataframe = bd.read_sql(query=query, billing_project_id=project_id, from_file=True)

    versions = get_existing_partitions(
        bucket_name=project_id,
        dataset_id=dataset_id,
        table_id=registo_familia_table_id,
        prefix=f"staging/{dataset_id}/{registo_familia_table_id}",
    )

    if dataframe is not None and not dataframe.empty:
        layout_versions = dataframe["versao_layout_particao"].tolist()
        not_in_layout_versions = [version for version in versions if version not in layout_versions]  # noqa
        if not_in_layout_versions:
            raise_msg = ""
            raise_msg += "Please, upload the layouts to the storage raw:\n"
            raise_msg += (
                "https://console.cloud.google.com/storage/browser/rj-smas/raw/protecao_social_cadunico/layout\n\n"
            )
            raise_msg += "layout versions to be uploaded\n{not_in_layout_versions}"

            raise ValueError(raise_msg)

        log(
            f"Dataframe will be filtered using versions from {project_id}.{dataset_id}_staging.{registo_familia_table_id}: {versions}"  # noqa
        )

        return dataframe[dataframe["versao_layout_particao"].isin(versions)]
    else:
        raise ValueError(f"Dataframe is None or empty: {dataframe}")


def columns_version_control_diff(dataframe: pd.DataFrame):
    """Analisa diferenças entre versões de layout com logging consolidado detalhado"""
    df_concat = pd.DataFrame()
    version_warnings = []  # Coletar warnings para log consolidado
    tables_dict = get_tables_names_dict()  # Obter nomes reais das tabelas

    for reg in dataframe["reg"].unique().tolist():
        df_table = dataframe[dataframe["reg"] == reg]
        table_versions = df_table["versao_layout_particao"].unique().tolist()
        table_versions = [table_versions[0]] + table_versions

        for i in range(len(table_versions) - 1):
            current_version = table_versions[i]
            df_version = df_table[df_table["versao_layout_particao"] == current_version]
            version_columns = df_version["column"].tolist()

            next_version = table_versions[i + 1]
            df_next_version = df_table[df_table["versao_layout_particao"] == next_version]
            next_version_columns = df_next_version["column"].tolist()

            control_column_version = []
            prev_versions = []
            missing_columns_in_table = []

            for nv_col in next_version_columns:
                prev_versions.append(current_version)
                if nv_col not in version_columns:
                    control_column_version.append("False")
                    missing_columns_in_table.append(nv_col)
                else:
                    control_column_version.append("True")

            # Registrar warnings detalhados da tabela se houver
            if missing_columns_in_table:
                # Obter nome real da tabela
                table_name = tables_dict.get(reg.zfill(2), f"reg_{reg}")

                # Criar warning detalhado com TODAS as colunas
                columns_list = ", ".join(missing_columns_in_table)
                warning_msg = f"Tabela {reg.zfill(2)} ({table_name}) v{next_version}: Colunas ausentes em v{current_version}: [{columns_list}]"
                version_warnings.append(warning_msg)

            df_next_version["versao_layout_anterior"] = prev_versions
            df_next_version["coluna_esta_versao_anterior"] = control_column_version

            df_concat = pd.concat([df_concat, df_next_version])

    # Log consolidado de todos os warnings encontrados (SEM OMISSÃO)
    if version_warnings:
        logger = PipelineLogger()
        logger.log_warning_summary("INCONSISTÊNCIAS ENTRE VERSÕES", version_warnings)

    return df_concat.sort_values(["reg", "versao_layout_particao"])


def parse_columns_version_control(dataframe: pd.DataFrame):
    log("ASCENDING SEARCH\n")
    df_ascending = columns_version_control_diff(dataframe=dataframe.sort_values(["reg", "versao_layout_particao"]))
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
        dataframe=dataframe.sort_values(["reg", "versao_layout_particao"], ascending=False)
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
    # log(f"Dumping schema to {schema_yaml_path}")
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
    dbt_repository_path: str,
    dataframe: pd.DataFrame,
    project_id: str,
    model_dataset_id: str,
    model_table_id: str,
):
    model_path = Path(dbt_repository_path) / "models/raw/smas/protecao_social_cadunico"
    model_name_prefix = "raw_protecao_social_cadunico"

    df = dataframe.copy()
    df["reg"] = df["reg"].apply(lambda x: x if len(x) > 1 else f"0{x}")
    df["version"] = df["version"].str.replace(".", "").apply(lambda x: x if len(x) > 3 else f"0{x}")
    df = df.sort_values(["reg", "versao_layout_particao"])
    df["version"] = np.where(
        df["coluna_esta_versao_anterior"] == "False",
        df["versao_layout_anterior"],
        df["version"],
    )
    df["dicionario_atributos"] = df["dicionario_atributos"].apply(lambda s: convert_string_to_json(s))

    # remove columns that are empty
    df = df[np.logical_not(df["column"].str.contains("vazio"))]
    df = df.sort_values(["reg", "versao_layout_particao"])
    tables_dict = get_tables_names_dict()
    log_created_models = []

    last_version = max(df["version"])
    log(f"Schema.yml will use the last version of the layout: {last_version}")

    for table_number in df["reg"].unique():
        schema = {"version": 2, "models": []}
        table_schema = {}
        table_model_name = tables_dict[table_number]
        model_name = table_model_name if "test" not in model_dataset_id else f"{table_model_name}_test"

        tables = df[df["reg"] == table_number]
        versions = tables["version"].unique()
        table_schema["name"] = f"{model_name_prefix}__{model_name}"
        table_schema["description"] = f"Table {model_name} from number {table_number}"
        table_schema["columns"] = []
        final_query = ""
        ini_query = """
            SELECT
        """
        end_query = """
                SAFE_CAST(versao_layout_particao AS STRING) AS versao_layout,
                SAFE_CAST(data_particao AS DATE) AS data_particao
            FROM `__project_id_replacer__.__dataset_id_replacer___staging.__model_table_id_replacer__`
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
            table_name = f"{table_name_original}_test" if "test" in model_dataset_id else table_name_original
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

                col_name_padronizado = col_name_padronizado if col_name_padronizado is not None else col_name
                dicionario_atributos = row["dicionario_atributos"]

                if dicionario_atributos is not None:
                    if "id_" in col_name_padronizado:
                        col_name_padronizado_dict_atr = col_name_padronizado.replace("id_", "", 1)
                    else:
                        raise Exception(f"col_name_padronizado: {col_name_padronizado} should have id_ in the name")

                if col_in_last_version == "False":
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + f"    NULL AS {col_name_padronizado}, --Essa coluna não esta na versao posterior"
                    )
                    if dicionario_atributos is not None:
                        col_expression = (
                            col_expression
                            + f"\n    --column: {column}\n"
                            + f"    NULL AS {col_name_padronizado_dict_atr}, --Essa coluna não esta na versao posterior"
                        )
                elif bigquery_type == "DATE":
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + "    SAFE.PARSE_DATE(\n"
                        + f"        '{date_format}',\n"
                        + "        CASE\n"
                        + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\\s*$') THEN NULL\n"
                        + f"            ELSE TRIM({col_name})\n"
                        + "        END"
                        + f"    ) AS {col_name_padronizado},"
                    )
                elif bigquery_type == "INT64":
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + "    SAFE_CAST(\n"
                        + "        CASE\n"
                        + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\\s*$') THEN NULL\n"
                        + f"            ELSE TRIM({col_name})\n"
                        + f"        END AS {bigquery_type}\n"
                        + f"    ) AS {col_name_padronizado},"
                    )
                elif bigquery_type == "FLOAT64":
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + "    SAFE_CAST(\n"
                        + "        CASE\n"
                        + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\\s*$') THEN NULL\n"
                        + f"            ELSE SAFE_CAST( TRIM({col_name}) AS INT64) / {ajuste_decimal}\n"
                        + f"        END AS {bigquery_type}\n"
                        + f"    ) AS {col_name_padronizado},"
                    )
                else:
                    col_expression = (
                        f"\n    --column: {column}\n"
                        + "    CAST(\n"
                        + "        CASE\n"
                        + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\\s*$') THEN NULL\n"
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
                            + f"            WHEN REGEXP_CONTAINS({col_name}, r'^\\s*$') THEN NULL\n"
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
                    col_description = row["descricao"] if row["descricao"] is not None else "Sem descrição"
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
            table_query = table_query.replace("__project_id_replacer__", project_id)
            table_query = table_query.replace("__dataset_id_replacer__", model_dataset_id)
            table_query = table_query.replace("__table_id_replacer__", table_name)
            table_query = table_query.replace("__table_number_replacer__", table_number)
            table_query = table_query.replace("__version_replacer__", version)
            table_query = table_query.replace("__model_table_id_replacer__", model_table_id)
            final_query += table_query
        final_query = final_query.rsplit("UNION ALL", 1)[0]
        for item in table_schema["columns"]:
            column_dict[item["name"]] = item
        table_schema["columns"] = list(column_dict.values())
        schema["models"].append(table_schema)

        sql_filepath = model_path / f"{model_name_prefix}__{model_name}.sql"

        sql_filepath.parent.mkdir(parents=True, exist_ok=True)
        log_created_models.append(str(sql_filepath))
        config_partition = """
                {{
                    config(
                        alias='__model_name__',
                        schema='protecao_social_cadunico',
                        materialized="table",
                        partition_by={
                            "field": "data_particao",
                            "data_type": "date",
                            "granularity": "month",
                        }
                    )
                }}

        """
        config_partition = config_partition.replace("__model_name__", model_name)
        final_query = textwrap.dedent(config_partition) + final_query

        with open(sql_filepath, "w") as text_file:
            text_file.write(final_query)

        schema_filepath = model_path / f"{model_name_prefix}__{model_name}.yml"
        dump_dict_to_dbt_yaml(schema=schema, schema_yaml_path=schema_filepath)
    json_log = json.dumps(log_created_models, indent=4)
    log(f"created {len(log_created_models)} prod models : {json_log}")


def create_layout_column_cross_version_control_bq_table(dataframe, dataset_id, table_id):
    new_dataframe = parse_columns_version_control(dataframe=dataframe)

    output_path = Path("/tmp/cadunico/final_layout")
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.mkdir(parents=True, exist_ok=True)

    to_partitions(
        data=new_dataframe,
        partition_columns=["versao_layout_particao"],
        savepath=str(output_path),
    )
    table_id = table_id + "_columns_version_control"
    create_table_and_upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        biglake_table=True,
        only_staging_dataset=True,
    )
    log(f"Table {dataset_id}.{table_id} created in STAGING")

    return new_dataframe


def update_layout_from_storage_and_create_versions_dbt_models(
    dataset_id: str = "brutos_cadunico",
    layout_table_id: str = "layout",
    registo_familia_table_id: str = "registro_familia",
    raw_bucket: str = "rj-smas",
    raw_prefix_area: str = "raw/protecao_social_cadunico/layout",
    staging_bucket: str = "rj-iplanrio",
    force_create_models: bool = True,
    repository_path: str = "/tmp/dbt_repository/",
):
    logger = PipelineLogger("Materialização CadÚnico")

    # FASE 1: Sincronização de Layouts
    logger.start_phase(
        "SINCRONIZAÇÃO DE LAYOUTS",
        {
            "dataset": dataset_id,
            "raw_bucket": raw_bucket,
            "staging_bucket": staging_bucket,
        },
    )
    staging_prefix_area = f"staging/{dataset_id}/{layout_table_id}"
    logger.log_progress("VERIFICANDO PARTIÇÕES EM STAGING")
    staging_partitions_list = get_existing_partitions(
        prefix=staging_prefix_area,
        bucket_name=staging_bucket,
        dataset_id=dataset_id,
        table_id=layout_table_id,
    )

    logger.log_progress("BUSCANDO NOVOS LAYOUTS EM RAW")
    raw_filespaths_to_ingest = download_files_from_storage_raw(
        raw_bucket=raw_bucket,
        raw_prefix_area=f"raw/protecao_social_cadunico/layout",
        staging_partitions_list=staging_partitions_list,
        output_path_str="/tmp/cadunico/raw/layout",
    )

    files_to_process = len(raw_filespaths_to_ingest) if raw_filespaths_to_ingest else 0

    if raw_filespaths_to_ingest:
        logger.log_progress("PROCESSANDO LAYOUTS NOVOS", {"arquivos": files_to_process})
        output_path = parse_xlsx_files_and_save_partition(
            output_path="/tmp/cadunico/staging/layout",
            raw_filespaths_to_ingest=raw_filespaths_to_ingest,
        )
        log(f"CRIANDO TABELA DE LAYOUT: {raw_bucket}.{dataset_id}.{layout_table_id}")
        output_path_result = create_table_and_upload_to_gcs(
            data_path=str(output_path),
            dump_mode="append",
            dataset_id=dataset_id,
            table_id=layout_table_id,
            only_staging_dataset=True,
        )
        output_path = str(output_path_result)
        logger.complete_phase(True, {"layouts_processados": files_to_process})
    else:
        logger.complete_phase(True, {"layouts_novos": 0, "status": "nenhum arquivo novo"})

    # FASE 2: Geração de Modelos DBT
    if raw_filespaths_to_ingest or force_create_models:
        logger.start_phase(
            "GERAÇÃO DE MODELOS DBT",
            {"força_criação": force_create_models, "repositório": repository_path},
        )

        logger.log_progress("CARREGANDO LAYOUT CONSOLIDADO DO STAGING")
        dataframe = get_layout_table_from_staging(
            project_id=staging_bucket,
            dataset_id=dataset_id,
            layout_table_id=layout_table_id,
            registo_familia_table_id=registo_familia_table_id,
        )

        logger.log_progress("CRIANDO CONTROLE DE VERSÕES CROSS-LAYOUT")
        df_final = create_layout_column_cross_version_control_bq_table(
            dataframe=dataframe, dataset_id=dataset_id, table_id=layout_table_id
        )

        logger.log_progress("GERANDO MODELOS DBT CONSOLIDADOS")
        create_cadunico_dbt_consolidated_models(
            project_id=staging_bucket,
            dbt_repository_path=repository_path,
            dataframe=df_final,
            model_dataset_id=dataset_id,
            model_table_id=registo_familia_table_id,
        )

        tables_dict = get_tables_names_dict()
        logger.complete_phase(
            True,
            {
                "tabelas_processadas": (len(df_final["reg"].unique()) if not df_final.empty else 0),
                "tipos_modelo": len(tables_dict),
            },
        )


def update_local_layout(
    dataset_id: str = "brutos_cadunico",
    layout_table_id: str = "layout",
    registo_familia_table_id: str = "registro_familia",
    raw_bucket: str = "rj-smas",
    raw_prefix_area: str = "raw/protecao_social_cadunico/layout",
    staging_bucket: str = "rj-iplanrio",
    force_create_models: bool = True,
    repository_path: str = "/tmp/dbt_repository/",
):
    update_layout_from_storage_and_create_versions_dbt_models(
        dataset_id=dataset_id,
        layout_table_id=layout_table_id,
        registo_familia_table_id=registo_familia_table_id,
        raw_bucket=raw_bucket,
        raw_prefix_area=raw_prefix_area,
        staging_bucket=staging_bucket,
        force_create_models=force_create_models,
        repository_path=repository_path,
    )
