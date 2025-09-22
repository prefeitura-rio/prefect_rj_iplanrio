# -*- coding: utf-8 -*-
"""
Tasks migradas do Prefect 1.4 para 3.0 - CRM API Wetalkie

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no iplanrio:
- handler_initialize_sentry: SEM EQUIVALENTE
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd
import requests
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.exceptions import TerminationSignal
from prefect.states import Completed
from pytz import timezone

from pipelines.rj_crm__api_wetalkie.utils.tasks import (
    AudioDownloadError,
    AudioProcessingError,
    AudioTranscriptionError,
    check_audio_duration,
    check_audio_file,
    download_audio,
    download_data_from_bigquery,
    transcribe_audio,
)


@task
def get_attendances(api: object) -> pd.DataFrame:
    """
    Get all attendances from the Wetalkie API
    """
    log("Getting all attendances from the Wetalkie API")
    all_attendances = api.get(path="/callcenter/attendances/pull")
    data = []

    log(f"all_attendances content: {all_attendances}")
    log(f"all_attendances type: {type(all_attendances)}")

    if hasattr(all_attendances, "json"):
        response_data = all_attendances.json()
        log(f"response_data after json(): {response_data}")
    else:
        response_data = all_attendances
        log(f"response_data (no json method): {response_data}")

    # Check if API returned a "message" response (indicates no data available)
    if "message" in response_data and "data" not in response_data:
        log(f"WeTalkie API returned message response (no data available): {response_data.get('message')}")
        return pd.DataFrame()  # Return empty DataFrame - no data to process

    all_attendances = response_data["data"]["items"]
    if not all_attendances:
        log("No attendances found in the Wetalkie API", level="warning")
        new_state = Completed(
            message="Flow new state"        )
        
        # Raise a TerminationSignal with the new state
        raise TerminationSignal(
            state=new_state
        )

    for item in all_attendances:
        data.append(
            {
                "end_date": item["endDate"],
                "begin_date": item["beginDate"],
                "ura_name": item["ura"]["name"],
                "id_ura": item["ura"]["id"],
                "channel": item["channel"].lower(),
                "id_reply": item["serial"],
                "protocol": item["protocol"],
                "json_data": item,
            }
        )
    log("End getting all attendances from the Wetalkie API")

    dfr = pd.DataFrame(data)
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
def create_dispatch_payload(campaign_name: str, cost_center_id: int, destinations: Union[List, pd.DataFrame]) -> Dict:
    """
    Cria o payload para o dispatch
    Exemplo da estrutura do payload:
    {
        "campaignName": "Campanha teste01",
        "costCenterId": 125,
        "destinations": [
            {
                "to": "5534998768975",
                "externalId": "3",
                "vars": {
                    "COD_VALIDACAO": "1234"
                }
            }
        ]
    }
    """
    return {
        "campaignName": campaign_name,
        "costCenterId": cost_center_id,
        "destinations": destinations,
    }


@task
def dispatch(api: object, id_hsm: int, dispatch_payload: dict, chunk: int = 1000) -> str:
    """
    Do a dispatch with chunking support
    Expected response:
    {"data":{"items":[{"externalId":null,"id":174}]},"message":"Created","statusCode":201}
    """
    dispatch_date = datetime.now(timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

    # Split destinations into chunks if needed
    destinations = dispatch_payload["destinations"]
    total_destinations = len(destinations)

    log(f"Total destinations to dispatch: {total_destinations}")
    log(f"Chunk size: {chunk}")

    # Process in chunks
    for i in range(0, total_destinations, chunk):
        chunk_destinations = destinations[i : i + chunk]
        chunk_payload = {
            "campaignName": dispatch_payload["campaignName"],
            "costCenterId": dispatch_payload["costCenterId"],
            "destinations": chunk_destinations,
        }

        log(f"Dispatching chunk {i // chunk + 1} with {len(chunk_destinations)} destinations")

        response = api.post(path=f"/callcenter/hsm/send/{id_hsm}", json=chunk_payload)

        if response.status_code != 201:
            error_msg = f"Falha no disparo do chunk {i // chunk + 1}: {response.text}"
            log(error_msg, level="error")
            response.raise_for_status()
            raise Exception(error_msg)

    log("Disparo realizado com sucesso!")
    return dispatch_date


@task
def create_dispatch_dfr(
    id_hsm: int,
    original_destinations: List,
    campaign_name: str,
    cost_center_id: int,
    dispatch_date: str,
) -> pd.DataFrame:
    """
    Salva o disparo no banco de dados
    """
    data = []
    for destination in original_destinations:
        row = {
            "id_hsm": id_hsm,
            "dispatch_date": dispatch_date,
            "campaignName": campaign_name,
            "costCenterId": cost_center_id,
            "to": (destination.get("to") if isinstance(destination, dict) else destination),
            "externalId": (destination.get("externalId", None) if isinstance(destination, dict) else None),
            "vars": (destination.get("vars", None) if isinstance(destination, dict) else None),
        }
        data.append(row)

    dfr = pd.DataFrame(data)
    dfr = dfr[
        [
            "id_hsm",
            "dispatch_date",
            "campaignName",
            "costCenterId",
            "to",
            "externalId",
            "vars",
        ]
    ]
    log(f"dfr content: {dfr}")
    return dfr


@task
def check_api_status(api: object) -> bool:
    """Verifica se a API está funcionando retornando status 200"""
    try:
        response = api.get("/")
        if response.status_code == 200:
            log("API está funcionando corretamente.")
            return True

        log(f"API retornou status {response.status_code}.", level="warning")
        return False
    except requests.exceptions.RequestException as error:
        log(f"Erro ao acessar a API: {error}", level="error")
        return False


@task
def check_has_query(query: str) -> bool:
    """
    Verifica se a query foi passada
    """
    if query:
        log("Query was found")
        return True
    log("No query was found")
    return False


@task
def printar(text):
    """exibe o texto passado como parâmetro"""
    log(f"Printando {text}")


@task
def get_destinations(destinations: Union[None, List[str]], query: str) -> List:
    """
    Get destinations from the query or from the parameter
    """
    if query:
        log("Query was found")
        destinations_df = download_data_from_bigquery(
            query=query,
            billing_project_id="rj-crm-registry",
            bucket_name="rj-crm-registry",
        )
        log(f"response from query {destinations_df.head()}")
        destinations = destinations_df.iloc[:, 0].tolist()
        destinations = [json.loads(item.replace("to_", "to")) for item in destinations]
        log(f"destinations inside get destinations {destinations}")
    elif isinstance(destinations, str):
        destinations = json.loads(destinations)
    return destinations or []


@task
def remove_duplicate_phones(destinations: List) -> List:
    """
    Remove duplicate phone numbers from destinations list
    """
    if not destinations:
        return []

    seen_phones = set()
    unique_destinations = []

    for dest in destinations:
        if isinstance(dest, dict):
            phone = dest.get("to", "")
        else:
            phone = str(dest)

        if phone not in seen_phones:
            seen_phones.add(phone)
            unique_destinations.append(dest)

    log(f"Removed {len(destinations) - len(unique_destinations)} duplicate phone numbers")
    return unique_destinations


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
