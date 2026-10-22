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
from google.cloud import bigquery, speech
from mutagen.mp3 import MP3
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from prefect import task
from prefect.logging import get_logger
from pytz import timezone

from utils.utils import read_env

logger = get_logger("toolkit")

audio_extensions = ["mp3", "wav", "ogg", "oga", "opus"]

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
            logger.info(f"Login successful to {login_url}")

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
                logger.info("Authentication token obtained successfully")
            else:
                logger.info("Warning: No token found in login response")
                logger.info(f"Response keys: {list(response_data.keys())}")

        except requests.RequestException as e:
            logger.info(f"Login failed: {e}")
            raise Exception(f"Failed to authenticate with API: {e}")

    def _refresh_token_if_needed(self, response):
        """Check if token needs refresh based on response status"""
        if response.status_code == 401:
            logger.info("Token expired, refreshing...")
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
        if original_extension not in audio_extensions:
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
        elif audio_format in audio_extensions:
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
    url = read_env(infisical_url)
    username = read_env(infisical_username)
    password = read_env(infisical_password)

    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)
    return api


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
def calculate_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    interval: Optional[int] = 7,
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
        today = datetime.now(tz).date()
        calculated_end_date = today
        calculated_start_date = calculated_end_date - timedelta(days=interval)

        result = {
            "start_date": calculated_start_date.strftime("%Y-%m-%d"),
            "end_date": calculated_end_date.strftime("%Y-%m-%d"),
        }

        logger.info(f"Calculated date range: {result['start_date']} to {result['end_date']}")
    else:
        result = {"start_date": start_date, "end_date": end_date}

        logger.info(f"Using provided date range: {result['start_date']} to {result['end_date']}")

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
    logger.info(f"Getting attendances from {start_date} to {end_date}")
    all_attendances = []
    page_number = 0
    page_size = 100  # api só aceita no máximo 100

    while True:
        logger.debug(f"🔍 Buscando página {page_number} (acumulados: {len(all_attendances)} atendimentos)")

        # Build path with matrix variables and query parameters
        path = f"/callcenter/attendances;beginDate={start_date};endDate={end_date}"
        # filtra pela data do fim de atendimento
        # period=(finalization , opening)
        params = {"pageSize": page_size, "pageNumber": page_number}

        response = api.get(path=path, params=params)

        if response.status_code != 200:
            logger.error(
                f"API request failed with status {response.status_code}: {response.text}",
            )
            response.raise_for_status()

        try:
            response_data = response.json()
        except Exception as e:
            logger.error(
                f"Failed to parse JSON response on page {page_number}: {e}",
            )
            raise

        # ===== LOGGING DETALHADO DA RESPOSTA =====
        logger.debug(f"📥 Resposta da API para página {page_number}:")
        logger.debug(f"  - Chaves raiz: {list(response_data.keys())}")

        if "data" in response_data:
            logger.debug(f"  - Chaves em 'data': {list(response_data['data'].keys())}")
            if "item" in response_data["data"]:
                item_data = response_data["data"]["item"]
                logger.debug(f"  - Chaves em 'item': {list(item_data.keys())}")

                # Log específico de hasNextPage
                has_next = item_data.get("hasNextPage", "KEY_NOT_FOUND")
                # log(f"  - hasNextPage: {has_next} (tipo: {type(has_next).__name__})", level="info")

                # Log de elements
                if "elements" in item_data:
                    elements = item_data["elements"]
                    logger.debug(f"  - elements: {len(elements) if elements else 0} itens")
                else:
                    logger.warning(f"  - elements: KEY_NOT_FOUND")
            else:
                logger.warning(f"  - 'item' não encontrado em 'data'")
        else:
            logger.warning(f"  - 'data' não encontrado na resposta")

        # Se houver 'message' na resposta
        if not "message" in response_data:
            logger.warning(f"  - No message key in response data: {list(response_data.keys())}")
        # =========================================

        page_attendances = []
        has_next_page = False  # Default seguro: parar se resposta malformada

        if "data" in response_data and "item" in response_data["data"]:
            item_data = response_data["data"]["item"]
            if "elements" in item_data:
                page_attendances = item_data["elements"] or []
            has_next_page = item_data.get("hasNextPage", False)

        # Log para detectar duplicação de páginas
        if page_attendances:
            first_id = page_attendances[0].get("id", "N/A")
            last_id = page_attendances[-1].get("id", "N/A") if len(page_attendances) > 1 else first_id
            logger.info(
                f"📄 Página {page_number}: {len(page_attendances)} atendimentos "
                f"(primeiro ID: {first_id}, último ID: {last_id}). Found {len(page_attendances)} attendances.")

        all_attendances.extend(page_attendances)

        # Log de decisão
        logger.info(
            f"🔄 Decisão de paginação após página {page_number}: "
            f"has_next_page={has_next_page}, total acumulado={len(all_attendances)}")

        if not has_next_page:
            logger.info(f"✓ Paginação finalizada: hasNextPage=False")
            break

        logger.debug(f"➡️ Continuando para página {page_number + 1}")
        page_number += 1

    if not all_attendances:
        logger.info("No attendances found")
        return pd.DataFrame()

    logger.info(f"✓ Total de atendimentos obtidos: {len(all_attendances)}")
    logger.info(f"Total attendances collected: {len(all_attendances)}")

    data = []
    for item in all_attendances:
        if item.get("ura"):
            ura_name = item.get("ura", {}).get("name")
            id_ura = item.get("ura", {}).get("id")
        elif item.get("flow"):
            ura_name = item.get("flow", {}).get("name")
            id_ura = item.get("flow", {}).get("id")
        else:
            ura_name, id_ura = None, None
        data.append(
            {
                "end_date": item.get("endDate"),
                "begin_date": item.get("beginDate"),
                "ura_name": ura_name,
                "id_ura": id_ura,
                "channel": (item.get("channel", "").lower() if item.get("channel") else None),
                "id_reply": item.get("serial"),
                "protocol": item.get("protocol"),
                "json_data": item,
            }
        )

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
            logger.warning(
                f"Registro {id_reply} sem 'json_data' válido ou não é um dicionário. Pulando.",
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
                            or any(url_audio.endswith(ext) for ext in audio_extensions)
                        )
                    ):
                        audio_encontrado = True
                        logger.info(f"Áudio encontrado para transcrição na sessão {id_reply}, mensagem ID {msg_copy.get('id')}")
                        audio_path_temp = None
                        try:
                            audio_path_temp = download_audio(url_audio)
                            check_audio_file(audio_path_temp)
                            check_audio_duration(audio_path_temp, max_duration_seconds)
                            transcricao = transcribe_audio(audio_path_temp)
                            msg_copy["text"] = transcricao
                            msg_copy["transcription_failed"] = 0
                            logger.info(
                                f"Transcrição concluída para sessão {id_reply}, msg {msg_copy.get('id')}: Status {'sucesso' if transcricao != 'Áudio sem conteúdo reconhecível' else 'sem_conteudo'}"
                            )

                        except (
                            AudioDownloadError,
                            AudioProcessingError,
                            AudioTranscriptionError,
                        ) as e:
                            erro_msg = f"ERRO_TRANSCRICAO: {type(e).__name__}: {e!s}"
                            logger.error(
                                f"Erro ao transcrever áudio sessão {id_reply}, msg {msg_copy.get('id')}: {erro_msg}. Audio url: {url_audio}",
                            )
                            msg_copy["text"] = erro_msg
                            msg_copy["transcription_failed"] = 1
                        except Exception as e:
                            erro_msg = f"ERRO_INESPERADO_TRANSCRICAO: {type(e).__name__}: {e!s}"
                            logger.error(
                                f"Erro inesperado ao processar áudio sessão {id_reply}, msg {msg_copy.get('id')}: {erro_msg}",
                            )
                            msg_copy["text"] = erro_msg
                            msg_copy["transcription_failed"] = 1
                        finally:
                            if audio_path_temp and os.path.exists(audio_path_temp):
                                try:
                                    os.unlink(audio_path_temp)
                                except Exception as e_unlink:
                                    logger.warning(
                                        f"Erro ao remover arquivo temporário {audio_path_temp}: {e_unlink}",
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
            logger.error(
                f"Erro inesperado ao processar registro {id_reply} (dict json_data): {e}",
            )
            dados_processados.append(registro)

    logger.info(f"Processamento JSON e transcrição concluídos para {len(dados_entrada)} registros.")
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
    import json

    df = pd.DataFrame(dados_processados)

    # Convert json_data dict to JSON string for proper storage
    if 'json_data' in df.columns:
        df['json_data'] = df['json_data'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    return df


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
    Uses composite key: id_ura + id_reply + protocol + begin_date + end_date
    because API filter is based on end_date

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
            CAST(DATE(begin_date) AS STRING), '|',
            CAST(DATE(end_date) AS STRING)
        ) as composite_key
        FROM `{billing_project_id}.{dataset_id}_staging.{table_id}`
        WHERE DATE(end_date) BETWEEN '{start_date}' AND '{end_date}'
    """

    logger.info(f"Checking existing attendance keys for period {start_date} to {end_date}")

    dfr = download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    if dfr.empty:
        logger.info("No existing attendance data found for the specified period")
        return []

    existing_keys = dfr["composite_key"].tolist()
    logger.info(f"Found {len(existing_keys)} existing attendance records")
    return existing_keys


@task
def filter_new_attendances(
    raw_attendances: pd.DataFrame,
    existing_keys: List[str],
) -> pd.DataFrame:
    """
    Filter attendances to include only those not already processed.
    Uses composite key: id_ura + id_reply + protocol + begin_date + end_date
    because API filter is based on end_date 

    Args:
        raw_attendances: DataFrame with raw attendance data
        existing_keys: List of composite keys already in BigQuery

    Returns:
        DataFrame with only new attendances
    """
    if raw_attendances.empty:
        logger.info("Raw attendances DataFrame is empty, nothing to filter")
        return raw_attendances

    if not existing_keys:
        logger.info("No existing keys found, keeping all attendances")
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
        + "|"
        + pd.to_datetime(raw_attendances["end_date"]).dt.strftime("%Y-%m-%d")
    )

    # Filter out attendances with keys that already exist
    new_attendances = raw_attendances[~raw_attendances["composite_key"].isin(existing_keys)]

    # Drop the temporary column
    new_attendances = new_attendances.drop(columns=["composite_key"])

    total_raw = len(raw_attendances)
    total_new = len(new_attendances)
    total_filtered = total_raw - total_new

    logger.info("Attendance filtering summary:")
    logger.info(f"  - Total raw attendances: {total_raw}")
    logger.info(f"  - Already processed: {total_filtered}")
    logger.info(f"  - New to process: {total_new}")

    return new_attendances
