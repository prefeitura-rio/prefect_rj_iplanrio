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
        log(
            f"Transcrição concluída com sucesso: {len(transcript)} caracteres",
            level="debug",
        )
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
    Get attendances from the Wetalkie API for a specific date range with pagination

    Args:
        api: Authenticated API handler
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with attendances data
    """
    log(f"Getting attendances from {start_date} to {end_date}")

    all_attendances = []
    page_number = 1
    page_size = 100

    while True:
        # Build path with matrix variables including pagination
        path = f"/callcenter/attendances;beginDate={start_date};endDate={end_date};pageSize={page_size};pageNumber={page_number}"

        log(f"Fetching page {page_number} with pageSize={page_size}")

        response = api.get(path=path)

        if response.status_code != 200:
            log(
                f"API request failed with status {response.status_code}: {response.text}",
                level="error",
            )
            response.raise_for_status()

        try:
            response_data = response.json()
            log(
                f"Page {page_number} response data structure: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}"
            )

            # Enhanced content logging for debugging
            if isinstance(response_data, dict):
                response_size = len(str(response_data))
                log(f"Page {page_number} response content size: {response_size} characters", level="debug")

                # Log sample of response content for debugging
                if response_size > 0:
                    sample_content = str(response_data)[:500] + "..." if response_size > 500 else str(response_data)
                    log(f"Page {page_number} response sample: {sample_content}", level="debug")
                else:
                    log(f"Page {page_number} response is empty dictionary", level="warning")
            elif isinstance(response_data, list):
                log(f"Page {page_number} response is a list with {len(response_data)} items", level="debug")
                if not response_data:
                    log(f"Page {page_number} response list is empty", level="warning")
            else:
                log(f"Page {page_number} response has unexpected type: {type(response_data)}", level="warning")
        except Exception as e:
            log(
                f"Failed to parse JSON response on page {page_number}: {e}",
                level="error",
            )
            raise

        # Enhanced check for API message responses with detailed diagnostics
        if "message" in response_data and "data" not in response_data:
            message = response_data.get('message', 'No message provided')
            status_code = response_data.get('statusCode', 'No status code')
            log(
                f"API returned message response on page {page_number} (no data available): {message}"
            )
            log(f"Message response details - statusCode: {status_code}, message: '{message}'", level="info")
            log(f"Full message response content: {response_data}", level="debug")
            log(f"Breaking pagination due to message-only response on page {page_number}", level="info")
            break

        # Enhanced check for API responses with both message and data
        if "message" in response_data and "data" in response_data:
            message = response_data.get('message', 'No message provided')
            status_code = response_data.get('statusCode', 'No status code')
            log(f"Page {page_number} API response includes message alongside data: '{message}' (status: {status_code})", level="info")

        # Enhanced check for empty response content
        if not response_data or (isinstance(response_data, dict) and not response_data):
            log(f"Page {page_number} returned empty/null content, breaking pagination", level="warning")
            log(f"Empty response details - Type: {type(response_data)}, Content: {response_data}", level="debug")
            log(f"Breaking pagination due to empty response on page {page_number}", level="info")
            break

        # Check for responses that look like error responses
        if isinstance(response_data, dict):
            error_indicators = ['error', 'Error', 'ERROR', 'errors']
            for indicator in error_indicators:
                if indicator in response_data:
                    error_content = response_data[indicator]
                    log(f"Page {page_number} response contains error indicator '{indicator}': {error_content}", level="warning")
                    log(f"Full error response: {response_data}", level="debug")

        # Extract attendances from response with enhanced validation and logging
        page_attendances = []
        has_next_page = False

        # Enhanced validation for data.item.elements structure
        if "data" in response_data and "item" in response_data["data"]:
            item_data = response_data["data"]["item"]
            log(
                f"Page {page_number} item_data keys: {list(item_data.keys()) if isinstance(item_data, dict) else 'Not a dict'}",
                level="debug",
            )

            if "elements" in item_data:
                elements = item_data["elements"]
                if elements is None:
                    log(f"Page {page_number} elements is None, treating as empty", level="warning")
                    page_attendances = []
                elif isinstance(elements, list):
                    page_attendances = elements
                    log(f"Page {page_number} extracted {len(elements)} elements from item_data", level="debug")
                else:
                    log(f"Page {page_number} elements has unexpected type: {type(elements)}", level="warning")
                    page_attendances = []
            else:
                log(f"Page {page_number} item_data does not contain 'elements' key", level="debug")

            has_next_page = item_data.get("hasNextPage", False)
            log(f"Page {page_number} hasNextPage value: {has_next_page}", level="info")

            # Enhanced diagnostics for API response structure
            log(f"Page {page_number} complete item_data structure:", level="debug")
            for key, value in item_data.items():
                if key == "elements":
                    log(f"  - {key}: list with {len(value) if isinstance(value, list) else 'not a list'} items", level="debug")
                else:
                    log(f"  - {key}: {type(value).__name__} = {value}", level="debug")

        # Enhanced validation for data.items structure
        elif "data" in response_data and "items" in response_data["data"]:
            items = response_data["data"]["items"]
            if items is None:
                log(f"Page {page_number} items is None, treating as empty", level="warning")
                page_attendances = []
            elif isinstance(items, list):
                page_attendances = items
                log(f"Page {page_number} extracted {len(items)} items from data.items", level="debug")
            else:
                log(f"Page {page_number} items has unexpected type: {type(items)}", level="warning")
                page_attendances = []

            # Log data structure for data.items path
            log(f"Page {page_number} data.items structure detected", level="debug")
            if "data" in response_data:
                data_keys = list(response_data["data"].keys())
                log(f"  - data keys: {data_keys}", level="debug")

        # Enhanced validation for direct list response
        elif isinstance(response_data, list):
            if not response_data:
                log(f"Page {page_number} received empty list response", level="warning")
            page_attendances = response_data
            log(f"Page {page_number} received direct list with {len(response_data)} items", level="debug")

        else:
            log(f"Page {page_number} response structure not recognized for data extraction", level="warning")
            log(
                f"Available keys in response: {list(response_data.keys()) if isinstance(response_data, dict) else 'Not a dict'}",
                level="debug",
            )

            # Enhanced diagnostics for unrecognized structure
            if isinstance(response_data, dict):
                log(f"Page {page_number} full response structure analysis:", level="debug")
                for key, value in response_data.items():
                    if isinstance(value, dict):
                        log(f"  - {key}: dict with keys {list(value.keys())}", level="debug")
                    elif isinstance(value, list):
                        log(f"  - {key}: list with {len(value)} items", level="debug")
                    else:
                        log(f"  - {key}: {type(value).__name__} = {value}", level="debug")

        # Enhanced empty content detection
        if not page_attendances:
            log(f"No attendances found on page {page_number}, ending pagination")
            log(f"Page {page_number} empty content details:", level="debug")
            log(f"  - Response data type: {type(response_data)}", level="debug")
            log(
                f"  - Has 'data' key: {'data' in response_data if isinstance(response_data, dict) else False}",
                level="debug",
            )
            log(
                f"  - Has 'item' in data: {'item' in response_data.get('data', {}) if isinstance(response_data, dict) else False}",
                level="debug",
            )
            log(
                f"  - Elements array length: {len(response_data.get('data', {}).get('item', {}).get('elements', [])) if isinstance(response_data, dict) else 'N/A'}",
                level="debug",
            )
            log(f"Breaking pagination due to empty page content on page {page_number}", level="info")
            break

        # Check if page_attendances contains only empty/null items
        valid_attendances = [att for att in page_attendances if att is not None and att != {}]
        if not valid_attendances:
            log(
                f"Page {page_number} contains only empty/null attendances ({len(page_attendances)} total), breaking pagination",
                level="warning",
            )
            log(f"Sample empty attendance items: {page_attendances[:3]}", level="debug")
            break

        log(f"Found {len(page_attendances)} attendances on page {page_number}")

        # Enhanced content diagnostics - log sample attendance and data patterns
        if page_attendances and len(page_attendances) > 0:
            # Log first attendance details
            sample_attendance = page_attendances[0]
            required_fields = ["endDate", "beginDate", "serial", "protocol"]
            sample_info = {field: sample_attendance.get(field, "MISSING") for field in required_fields}
            log(f"Page {page_number} sample attendance fields: {sample_info}", level="debug")

            # Log additional diagnostic info about attendance content
            log(f"Page {page_number} content diagnostics:", level="debug")
            log(f"  - First attendance keys: {list(sample_attendance.keys()) if isinstance(sample_attendance, dict) else 'Not a dict'}", level="debug")
            log(f"  - First attendance ID/Serial: {sample_attendance.get('serial', 'NO_SERIAL')}", level="debug")

            # Check if all attendances in this page have the same structure
            if len(page_attendances) > 1:
                last_attendance = page_attendances[-1]
                log(f"  - Last attendance ID/Serial: {last_attendance.get('serial', 'NO_SERIAL')}", level="debug")
                log(f"  - Same structure as first: {set(sample_attendance.keys()) == set(last_attendance.keys()) if isinstance(last_attendance, dict) else 'N/A'}", level="debug")

        all_attendances.extend(page_attendances)

        # Enhanced pagination decision logging with more diagnostic info
        log(f"Page {page_number} pagination decision analysis:", level="info")
        log(f"  - Current page size: {len(page_attendances)}", level="info")
        log(f"  - hasNextPage from API: {has_next_page}", level="info")
        log(f"  - Total attendances so far: {len(all_attendances)}", level="info")
        log(f"  - Requested pageSize: {page_size}", level="info")
        log(f"  - Page size vs requested: {len(page_attendances)}/{page_size} = {(len(page_attendances)/page_size)*100:.1f}%", level="info")

        # Log the actual message from API if present
        if isinstance(response_data, dict) and "message" in response_data:
            api_message = response_data.get("message", "No message")
            log(f"  - API message: {api_message}", level="info")

        # Log statusCode if present
        if isinstance(response_data, dict) and "statusCode" in response_data:
            status_code = response_data.get("statusCode", "No status code")
            log(f"  - API status code: {status_code}", level="info")

        # Check if there's a next page
        if not has_next_page:
            log("API indicates no next page (hasNextPage=false), ending pagination")
            log(
                f"Final pagination summary: collected {len(all_attendances)} attendances across {page_number} pages",
                level="info",
            )
            break

        # Enhanced safety check with more detailed analysis
        if len(page_attendances) < page_size * 0.5 and len(page_attendances) > 0:
            log(f"Page {page_number} returned {len(page_attendances)} attendances, which is less than 50% of page_size ({page_size}). This might indicate approaching end of data.", level="warning")
            log(f"  - Small page analysis: hasNextPage={has_next_page}, actual_size={len(page_attendances)}, expected_size={page_size}", level="warning")

            # Log if this is a consistent pattern
            if page_number > 5:  # Only log this after a few pages
                log(f"  - This is page {page_number} with small size - check if this is a consistent pattern indicating API limitation", level="warning")

        page_number += 1
        log(f"Moving to page {page_number} (hasNextPage={has_next_page})", level="info")

    if not all_attendances:
        log("No attendances found in any page", level="warning")
        log(f"Pagination completed after checking {page_number} pages with no data found", level="info")
        return pd.DataFrame()

    log(f"Total attendances collected across {page_number} pages: {len(all_attendances)}")
    log(f"Successful pagination completion - processed pages 1 through {page_number}", level="info")

    data = []
    for item in all_attendances:
        data.append(
            {
                "end_date": item.get("endDate"),
                "begin_date": item.get("beginDate"),
                "ura_name": (item.get("ura", {}).get("name") if item.get("ura") else None),
                "id_ura": item.get("ura", {}).get("id") if item.get("ura") else None,
                "channel": (item.get("channel", "").lower() if item.get("channel") else None),
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


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return results as a pandas DataFrame.

    Args:
        query (str): SQL query to execute in BigQuery
        billing_project_id (str): GCP project ID for billing purposes
        bucket_name (str): GCS bucket name for credential loading

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame
    """
    from time import sleep

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


@task
def get_existing_attendance_keys(
    dataset_id: str,
    table_id: str,
    start_date: str,
    end_date: str,
    billing_project_id: str,
) -> List[str]:
    """
    Get existing attendance keys from BigQuery table to avoid duplicates.
    Uses composite key: id_ura + id_reply + protocol + begin_date

    Args:
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        billing_project_id: GCP project ID for billing

    Returns:
        List of composite keys that already exist in the table
    """
    query = f"""
        SELECT DISTINCT CONCAT(
            CAST(id_ura AS STRING), '|',
            CAST(id_reply AS STRING), '|',
            COALESCE(protocol, 'NULL'), '|',
            CAST(DATE(begin_date) AS STRING)
        ) as composite_key
        FROM `{billing_project_id}.{dataset_id}_staging.{table_id}`
        WHERE DATE(begin_date) BETWEEN '{start_date}' AND '{end_date}'
    """

    log(f"Checking existing attendance keys for period {start_date} to {end_date}")

    dfr = download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    if dfr.empty:
        log("No existing attendance data found for the specified period")
        return []

    existing_keys = dfr["composite_key"].tolist()
    log(f"Found {len(existing_keys)} existing attendance records")
    return existing_keys


@task
def filter_new_attendances(
    raw_attendances: pd.DataFrame,
    existing_keys: List[str],
) -> pd.DataFrame:
    """
    Filter attendances to include only those not already processed.
    Uses composite key: id_ura + id_reply + protocol + begin_date

    Args:
        raw_attendances: DataFrame with raw attendance data
        existing_keys: List of composite keys already in BigQuery

    Returns:
        DataFrame with only new attendances
    """
    if raw_attendances.empty:
        log("Raw attendances DataFrame is empty, nothing to filter")
        return raw_attendances

    if not existing_keys:
        log("No existing keys found, keeping all attendances")
        return raw_attendances

    # Create composite key for each raw attendance
    raw_attendances["composite_key"] = (
        raw_attendances["id_ura"].astype(str)
        + "|"
        + raw_attendances["id_reply"].astype(str)
        + "|"
        + raw_attendances["protocol"].fillna("NULL").astype(str)
        + "|"
        + pd.to_datetime(raw_attendances["begin_date"]).dt.strftime("%Y-%m-%d")
    )

    # Filter out attendances with keys that already exist
    new_attendances = raw_attendances[~raw_attendances["composite_key"].isin(existing_keys)]

    # Drop the temporary column
    new_attendances = new_attendances.drop(columns=["composite_key"])

    total_raw = len(raw_attendances)
    total_new = len(new_attendances)
    total_filtered = total_raw - total_new

    log("Attendance filtering summary:")
    log(f"  - Total raw attendances: {total_raw}")
    log(f"  - Already processed: {total_filtered}")
    log(f"  - New to process: {total_new}")

    return new_attendances
