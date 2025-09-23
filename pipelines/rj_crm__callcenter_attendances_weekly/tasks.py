# -*- coding: utf-8 -*-
"""
Tasks para pipeline CRM Call Center Attendances Weekly
Código consolidado e independente
"""

import io
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict, List, Literal, Optional, Union
from urllib.request import urlretrieve

import pandas as pd
import requests
from basedosdados import Base
from google.cloud import bigquery, speech
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from mutagen.mp3 import MP3
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from prefect import task
from prefect.exceptions import PrefectException
from pytz import timezone


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


class ApiHandler:
    """
    Handles API authentication and request management with automatic token refresh.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        login_route: str = "users/login",
        token_type: str = "Bearer",
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.login_route = login_route.lstrip("/")
        self.token_type = token_type
        self.token = None
        self.headers = {"Content-Type": "application/json"}

        # Perform initial login
        self._login()

    def _login(self):
        """Perform login and extract token from response"""
        login_url = f"{self.base_url}/{self.login_route}"
        login_data = {"username": self.username, "password": self.password}

        try:
            response = requests.post(login_url, json=login_data, timeout=30)
            response.raise_for_status()

            response_data = response.json()
            log(f"Login successful to {login_url}")

            # Try to extract token from various common response patterns
            token = None
            if "token" in response_data:
                token = response_data["token"]
            elif "access_token" in response_data:
                token = response_data["access_token"]
            elif "authToken" in response_data:
                token = response_data["authToken"]
            elif "jwt" in response_data:
                token = response_data["jwt"]
            elif (
                "data" in response_data and "item" in response_data["data"] and "token" in response_data["data"]["item"]
            ):
                token = response_data["data"]["item"]["token"]

            if token:
                self.token = token
                self.headers["Authorization"] = f"{self.token_type} {token}"
                log("Authentication token obtained successfully")
            else:
                log("Warning: No token found in login response")
                log(f"Response keys: {list(response_data.keys())}")

        except requests.RequestException as e:
            log(f"Login failed: {e}")
            raise Exception(f"Failed to authenticate with API: {e}")

    def _refresh_token_if_needed(self, response):
        """Check if token needs refresh based on response status"""
        if response.status_code == 401:
            log("Token expired, refreshing...")
            self._login()
            return True
        return False

    def get(self, path: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        """Perform GET request with automatic token refresh"""
        url = f"{self.base_url}/{path.lstrip('/')}"

        response = requests.get(url, headers=self.headers, params=params, **kwargs)

        if self._refresh_token_if_needed(response):
            # Retry with new token
            response = requests.get(url, headers=self.headers, params=params, **kwargs)

        return response

    def post(
        self,
        path: str,
        json: Optional[Dict] = None,
        data: Optional[Any] = None,
        **kwargs,
    ) -> requests.Response:
        """Perform POST request with automatic token refresh"""
        url = f"{self.base_url}/{path.lstrip('/')}"

        response = requests.post(url, headers=self.headers, json=json, data=data, **kwargs)

        if self._refresh_token_if_needed(response):
            # Retry with new token
            response = requests.post(url, headers=self.headers, json=json, data=data, **kwargs)

        return response


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
def calculate_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, str]:
    """
    Calculate date range for attendances query.
    If start_date and end_date are None, calculate last 7 days period.

    Args:
        start_date: Start date in YYYY-MM-DD format or None
        end_date: End date in YYYY-MM-DD format or None

    Returns:
        Dictionary with 'start_date' and 'end_date' keys
    """
    tz = timezone("America/Sao_Paulo")

    if start_date is None or end_date is None:
        # Calculate last 7 days (from 8 days ago to 1 day ago)
        today = datetime.now(tz).date()
        calculated_end_date = today - timedelta(days=1)
        calculated_start_date = calculated_end_date - timedelta(days=6)

        result = {
            "start_date": calculated_start_date.strftime("%Y-%m-%d"),
            "end_date": calculated_end_date.strftime("%Y-%m-%d"),
        }

        log(f"Calculated date range: {result['start_date']} to {result['end_date']}")
    else:
        result = {"start_date": start_date, "end_date": end_date}

        log(f"Using provided date range: {result['start_date']} to {result['end_date']}")

    return result


@task
def get_weekly_attendances(api: object, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get attendances from the Wetalkie API for a specific date range

    Args:
        api: Authenticated API handler
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with attendances data
    """
    log(f"Getting attendances from {start_date} to {end_date}")

    # Build path with matrix variables (API expects beginDate/endDate as matrix variables)
    path = f"/callcenter/attendances;beginDate={start_date};endDate={end_date}"

    response = api.get(path=path)

    if response.status_code != 200:
        log(f"API request failed with status {response.status_code}: {response.text}", level="error")
        response.raise_for_status()

    log(f"API response status: {response.status_code}")

    try:
        response_data = response.json()
        log(
            f"Response data structure: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}"
        )
    except Exception as e:
        log(f"Failed to parse JSON response: {e}", level="error")
        raise

    # Check if API returned a "message" response (indicates no data available)
    if "message" in response_data and "data" not in response_data:
        log(f"API returned message response (no data available): {response_data.get('message')}")
        return pd.DataFrame()  # Return empty DataFrame - no data to process

    # Extract attendances from response
    if "data" in response_data and "items" in response_data["data"]:
        attendances = response_data["data"]["items"]
    elif isinstance(response_data, list):
        attendances = response_data
    else:
        log(f"Unexpected response structure: {response_data}", level="warning")
        return pd.DataFrame()

    if not attendances:
        log("No attendances found in the API response", level="warning")
        return pd.DataFrame()

    # Process attendances data
    data = []
    for item in attendances:
        data.append(
            {
                "end_date": item.get("endDate"),
                "begin_date": item.get("beginDate"),
                "ura_name": item.get("ura", {}).get("name") if item.get("ura") else None,
                "id_ura": item.get("ura", {}).get("id") if item.get("ura") else None,
                "channel": item.get("channel", "").lower() if item.get("channel") else None,
                "id_reply": item.get("serial"),
                "protocol": item.get("protocol"),
                "json_data": item,
            }
        )

    log(f"Processed {len(data)} attendances")

    dfr = pd.DataFrame(data)

    if not dfr.empty:
        dfr = dfr[
            [
                "id_ura",
                "id_reply",
                "ura_name",
                "protocol",
                "channel",
                "begin_date",
                "end_date",
                "json_data",
            ]
        ]

    return dfr


@task
def processar_json_e_transcrever_audios(
    dados_entrada: Union[pd.DataFrame, List[Dict[str, Any]]],
    max_duration_seconds: int = 300,
) -> List[Dict[str, Any]]:
    """
    Processa uma lista de registros ou um DataFrame, transcrevendo áudios encontrados no JSON.

    Args:
        dados_entrada: Lista de dicionários ou DataFrame, cada um contendo 'json_data'.
        max_duration_seconds: Duração máxima permitida para os áudios.

    Returns:
        Lista de dicionários com o campo 'json_data' modificado (campo 'texto' das mensagens de áudio preenchido).
    """
    dados_processados = []

    if isinstance(dados_entrada, pd.DataFrame):
        dados_entrada = dados_entrada.to_dict("records")

    for registro in dados_entrada:
        data = registro.get("json_data")
        id_reply = registro.get("id_reply", "ID_Not_Found")

        if not data or not isinstance(data, dict):
            log(
                f"Registro {id_reply} sem 'json_data' válido ou não é um dicionário. Pulando.",
                level="warning",
            )
            dados_processados.append(registro)
            continue

        try:
            mensagens = data.get("messages", [])
            mensagens_atualizadas = []
            audio_encontrado = False

            for msg in mensagens:
                msg_copy = msg.copy()
                media = msg_copy.get("media")
                texto_original = msg_copy.get("text")
                url_audio = None

                if media and isinstance(media, dict):
                    url_audio = media.get("file")
                    content_type = media.get("contentType", "").lower()

                    if (
                        url_audio
                        and not texto_original
                        and (
                            "audio" in content_type
                            or any(url_audio.endswith(ext) for ext in [".mp3", ".wav", ".ogg", ".oga", ".opus"])
                        )
                    ):
                        audio_encontrado = True
                        log(f"Áudio encontrado para transcrição na sessão {id_reply}, mensagem ID {msg_copy.get('id')}")
                        audio_path_temp = None
                        try:
                            audio_path_temp = download_audio(url_audio)
                            check_audio_file(audio_path_temp)
                            check_audio_duration(audio_path_temp, max_duration_seconds)
                            transcricao = transcribe_audio(audio_path_temp)
                            msg_copy["text"] = transcricao
                            log(
                                f"Transcrição concluída para sessão {id_reply}, msg {msg_copy.get('id')}: Status {'sucesso' if transcricao != 'Áudio sem conteúdo reconhecível' else 'sem_conteudo'}"
                            )

                        except (
                            AudioDownloadError,
                            AudioProcessingError,
                            AudioTranscriptionError,
                        ) as e:
                            erro_msg = f"ERRO_TRANSCRICAO: {type(e).__name__}: {e!s}"
                            log(
                                f"Erro ao transcrever áudio sessão {id_reply}, msg {msg_copy.get('id')}: {erro_msg}",
                                level="error",
                            )
                            msg_copy["text"] = None
                        except Exception as e:
                            erro_msg = f"ERRO_INESPERADO_TRANSCRICAO: {type(e).__name__}: {e!s}"
                            log(
                                f"Erro inesperado ao processar áudio sessão {id_reply}, msg {msg_copy.get('id')}: {erro_msg}",
                                level="error",
                            )
                            msg_copy["text"] = None
                        finally:
                            if audio_path_temp and os.path.exists(audio_path_temp):
                                try:
                                    os.unlink(audio_path_temp)
                                except Exception as e_unlink:
                                    log(
                                        f"Erro ao remover arquivo temporário {audio_path_temp}: {e_unlink}",
                                        level="warning",
                                    )

                mensagens_atualizadas.append(msg_copy)
            if audio_encontrado:
                data["messages"] = mensagens_atualizadas
                registro_atualizado = registro.copy()
                registro_atualizado["json_data"] = data
                dados_processados.append(registro_atualizado)
            else:
                dados_processados.append(registro)

        except Exception as e:
            log(
                f"Erro inesperado ao processar registro {id_reply} (dict json_data): {e}",
                level="error",
            )
            dados_processados.append(registro)

    log(f"Processamento JSON e transcrição concluídos para {len(dados_entrada)} registros.")
    return dados_processados


@task
def criar_dataframe_de_lista(dados_processados: list) -> pd.DataFrame:
    """
    Converts a list of processed data into a pandas DataFrame.

    Args:
        dados_processados: List of dictionaries containing processed data

    Returns:
        A pandas DataFrame created from the input list
    """
    return pd.DataFrame(dados_processados)