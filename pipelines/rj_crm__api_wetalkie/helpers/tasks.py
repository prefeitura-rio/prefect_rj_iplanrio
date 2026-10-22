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
from google.cloud import bigquery, speech
from mutagen.mp3 import MP3
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from prefect import task
from prefect.logging import get_logger

from pipelines.rj_crm__api_wetalkie.helpers.api_handler import ApiHandler
from utils.settings import WetalkieSettings

logger = get_logger("toolkit")


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
def access_api(login_route: str = "users/login") -> ApiHandler:
    """Return an authenticated handler of the Wetalkie API."""
    settings = WetalkieSettings()

    return ApiHandler(
        base_url=settings.WETALKIE_URL,
        username=settings.WETALKIE_USERNAME,
        password=settings.WETALKIE_PASSWORD.get_secret_value(),
        login_route=login_route,
    )


@task
def create_date_partitions(
    dataframe,
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Save a DataFrame in one file, with the columns `ano_particao`, `mes_particao` and `data_particao`."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dataframe["ano_particao"] = dataframe["data_particao"].str[:4]
    dataframe["mes_particao"] = dataframe["data_particao"].str[5:7]
    columns = [c for c in dataframe.columns if c not in ("ano_particao", "mes_particao", "data_particao")]
    dataframe = dataframe[[*columns, "ano_particao", "mes_particao", "data_particao"]]

    os.makedirs(root_folder, exist_ok=True)
    file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{file_format}")

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    else:
        dataframe.to_parquet(file_path, index=False)

    logger.info("Files saved on %s", root_folder)
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
        logger.info(message)
        return None
    return data


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """Execute a BigQuery SQL query and return results as a pandas DataFrame."""
    logger.info("Querying data from BigQuery")
    query = str(query)
    bq_client = bigquery.Client(
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    logger.info("Getting result from query")
    results = job.result()
    logger.info("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    logger.info("End download data from bigquery")
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
        logger.debug(f"Download concluído com sucesso para {temp_path}")
        return temp_path
    except ValueError as ve:
        raise ve
    except Exception as e:
        if "temp_path" in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        logger.error(f"Falha ao baixar áudio de {url}: {e}")
        raise AudioDownloadError(f"Falha ao baixar áudio de {url}: {e}") from e


def check_audio_file(audio_path: str) -> bool:
    """Check if audio file exists and is not empty."""
    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Arquivo de áudio não encontrado no caminho: {audio_path}")

    if os.path.getsize(audio_path) == 0:
        raise AudioProcessingError(f"Arquivo de áudio está vazio: {audio_path}")

    logger.debug(f"Verificação do arquivo de áudio concluída para: {audio_path}")
    return True


def check_audio_duration(audio_path: str, max_duration_seconds: int) -> None:
    """
    Check if audio duration is within specified limit.
    If duration cannot be determined, logs warning and continues (Google API will validate).
    """
    audio_format = audio_path.lower().split(".")[-1]
    duration = None

    logger.debug(f"Iniciando verificação de duração: {audio_path} (formato: {audio_format})")

    try:
        logger.debug(f"Tentando verificar duração com mutagen para formato {audio_format}")

        if audio_format == "mp3":
            audio = MP3(audio_path)
            duration = audio.info.length
        elif audio_format == "wav":
            audio = WAVE(audio_path)
            duration = audio.info.length
        elif audio_format in ["ogg", "oga", "opus"]:
            # Try Opus first (common for WhatsApp voice messages), then Vorbis
            try:
                audio = OggOpus(audio_path)
                duration = audio.info.length
                logger.debug(f"Detected Opus codec for {audio_path}")
            except Exception:
                try:
                    audio = OggVorbis(audio_path)
                    duration = audio.info.length
                    logger.debug(f"Detected Vorbis codec for {audio_path}")
                except Exception as e:
                    logger.warning(f"Could not detect OGG codec: {e}")
                    duration = None
        else:
            logger.warning(f"Formato de áudio não reconhecido: {audio_format}")
            duration = None

        if duration is not None:
            logger.info(
                f"✓ Duração verificada com mutagen: {duration:.2f}s para {audio_path}")
    except Exception as mutagen_error:
        logger.warning(
            f"⚠ Não foi possível verificar duração com mutagen para {audio_path} "
            f"({audio_format}): {mutagen_error}. "
            f"Continuando sem validação de duração - Google API validará.")
        duration = None

    # Se conseguiu determinar duração, valida o limite
    if duration is not None:
        if duration > max_duration_seconds:
            logger.error(
                f"✗ Áudio {audio_path} excede limite de duração: {duration:.2f}s > {max_duration_seconds}s")
            raise AudioProcessingError(
                f"Duração do áudio ({duration:.2f}s) excede o limite de {max_duration_seconds}s"
            )

        logger.debug(
            f"✓ Verificação de duração completa: {audio_path} ({duration:.2f}s <= {max_duration_seconds}s)")
    else:
        logger.info(
            f"⚠ Pulando validação de duração para {audio_path} - será validado pela Google API")


def transcribe_audio(audio_path: str, language_code: str = "pt-BR") -> str:
    """Transcribe audio file using Google Cloud Speech-to-Text API."""
    try:
        client = speech.SpeechClient()

        with io.open(audio_path, "rb") as audio_file:
            content = audio_file.read()

        audio = speech.RecognitionAudio(content=content)

        # Detect encoding based on file format
        audio_format = audio_path.lower().split(".")[-1]

        # Default config
        encoding = speech.RecognitionConfig.AudioEncoding.ENCODING_UNSPECIFIED
        sample_rate = 16000

        # Check if file is Opus (common in WhatsApp voice messages)
        if audio_format in ["ogg", "oga", "opus"]:
            try:
                opus_info = OggOpus(audio_path)
                encoding = speech.RecognitionConfig.AudioEncoding.OGG_OPUS
                sample_rate = opus_info.info.sample_rate  # Read from file metadata
                logger.info(f"Using OGG_OPUS encoding with {sample_rate}Hz for {audio_path}")
            except Exception:
                # Not Opus, let Google auto-detect
                logger.debug(f"Not Opus, using ENCODING_UNSPECIFIED for {audio_path}")

        config = speech.RecognitionConfig(
            encoding=encoding,
            sample_rate_hertz=sample_rate,
            language_code=language_code,
            enable_automatic_punctuation=True,
        )

        response = client.recognize(config=config, audio=audio)

        if not response.results:
            logger.warning("Nenhum resultado de transcrição encontrado")
            return "Áudio sem conteúdo reconhecível"

        transcript = ""
        for result in response.results:
            transcript += result.alternatives[0].transcript + " "

        transcript = transcript.strip()
        logger.debug(
            f"Transcrição concluída com sucesso: {len(transcript)} caracteres",
        )
        return transcript

    except Exception as e:
        logger.error(f"Erro durante a transcrição de áudio {audio_path}: {e}")
        raise AudioTranscriptionError(f"Falha na transcrição de áudio: {e}") from e
