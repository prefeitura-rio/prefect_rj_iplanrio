# -*- coding: utf-8 -*-
"""
Utility tasks for CRM API Wetalkie pipeline
Migrated and adapted from pipelines_rj_crm_registry
"""

import io
import os
import tempfile
import uuid
from datetime import datetime
from time import sleep
from typing import Dict, List, Literal, Union
from urllib.request import urlretrieve

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery, speech
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from mutagen.mp3 import MP3
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from prefect import task

from pipelines.rj_crm__api_wetalkie.utils.api_handler import ApiHandler


# Audio processing exceptions
class AudioDownloadError(IOError):
    """Exceção para falhas no download de áudio."""

    pass


class AudioProcessingError(ValueError):
    """Exceção para erros durante o processamento de áudio."""

    pass


class AudioTranscriptionError(Exception):
    """Exceção para falhas na transcrição de áudio."""

    pass


@task
def access_api(
    infisical_path: str,
    infisical_url: str,
    infisical_username: str,
    infisical_password: str,
    login_route: str = "users/login",
) -> ApiHandler:
    """
    Access API and return authenticated handler to be used in other requests.
    """
    url = getenv_or_action(infisical_url)
    username = getenv_or_action(infisical_username)
    password = getenv_or_action(infisical_password)

    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)
    return api


@task
def create_date_partitions(
    dataframe,
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Create date partitions for a DataFrame and save them to disk."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]),
        )
        for date in dates
    ]

    for _date, _dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            _dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            _dataframe.to_parquet(file_folder, index=False)

    log(f"Files saved on {root_folder}")
    return root_folder


@task
def skip_flow_if_empty(
    data: Union[pd.DataFrame, List, str, Dict],
    message: str = "Data is empty. Skipping flow.",
) -> Union[pd.DataFrame, List, str, Dict, None]:
    """Skip the flow if input data is empty.
    To skip is necessary to add the following check in the flow:
    'if validated_destinations is None:
        return  # flow termina aqui, nada downstream é agendado'
    """
    if len(data) == 0:
        log(message)
        return None
    return data


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """Execute a BigQuery SQL query and return results as a pandas DataFrame."""
    log("Querying data from BigQuery")
    query = str(query)
    bq_client = bigquery.Client(
        credentials=Base(bucket_name=bucket_name)._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    log("End download data from bigquery")
    return dfr


# Audio processing functions
def download_audio(url: str) -> str:
    """Download an audio file from URL to temporary local path."""
    try:
        original_extension = url.lower().split(".")[-1]
        if original_extension not in ["mp3", "wav", "ogg", "oga", "opus"]:
            raise ValueError(f"URL não possui uma extensão de áudio suportada: {url}")

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{original_extension}")
        temp_path = temp_file.name
        temp_file.close()

        urlretrieve(url, temp_path)
        log(f"Download concluído com sucesso para {temp_path}", level="debug")
        return temp_path
    except ValueError as ve:
        raise ve
    except Exception as e:
        if "temp_path" in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        log(f"Falha ao baixar áudio de {url}: {e}", level="error")
        raise AudioDownloadError(f"Falha ao baixar áudio de {url}: {e}") from e


def check_audio_file(audio_path: str) -> bool:
    """Check if audio file exists and is not empty."""
    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Arquivo de áudio não encontrado no caminho: {audio_path}")

    if os.path.getsize(audio_path) == 0:
        raise AudioProcessingError(f"Arquivo de áudio está vazio: {audio_path}")

    log(f"Verificação do arquivo de áudio concluída para: {audio_path}", level="debug")
    return True


def check_audio_duration(audio_path: str, max_duration_seconds: int) -> None:
    """Check if audio duration is within specified limit."""
    audio_format = audio_path.lower().split(".")[-1]
    duration = 0

    try:
        if audio_format == "mp3":
            audio = MP3(audio_path)
            duration = audio.info.length
        elif audio_format == "wav":
            audio = WAVE(audio_path)
            duration = audio.info.length
        elif audio_format in ["ogg", "oga"]:
            audio = OggVorbis(audio_path)
            duration = audio.info.length
        elif audio_format == "opus":
            audio = OggOpus(audio_path)
            duration = audio.info.length
        else:
            raise AudioProcessingError(f"Formato de áudio não suportado: {audio_format}")

        if duration > max_duration_seconds:
            raise AudioProcessingError(f"Duração do áudio ({duration:.2f}s) excede o limite de {max_duration_seconds}s")

        log(f"Duração do áudio verificada: {duration:.2f}s", level="debug")

    except Exception as e:
        log(f"Erro ao verificar duração do áudio {audio_path}: {e}", level="error")
        raise AudioProcessingError(f"Erro ao verificar duração do áudio: {e}") from e


def transcribe_audio(audio_path: str, language_code: str = "pt-BR") -> str:
    """Transcribe audio file using Google Cloud Speech-to-Text API."""
    try:
        client = speech.SpeechClient()

        with io.open(audio_path, "rb") as audio_file:
            content = audio_file.read()

        audio = speech.RecognitionAudio(content=content)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.ENCODING_UNSPECIFIED,
            sample_rate_hertz=16000,
            language_code=language_code,
            enable_automatic_punctuation=True,
        )

        response = client.recognize(config=config, audio=audio)

        if not response.results:
            log("Nenhum resultado de transcrição encontrado", level="warning")
            return "Áudio sem conteúdo reconhecível"

        transcript = ""
        for result in response.results:
            transcript += result.alternatives[0].transcript + " "

        transcript = transcript.strip()
        log(f"Transcrição concluída com sucesso: {len(transcript)} caracteres", level="debug")
        return transcript

    except Exception as e:
        log(f"Erro durante a transcrição de áudio {audio_path}: {e}", level="error")
        raise AudioTranscriptionError(f"Falha na transcrição de áudio: {e}") from e
