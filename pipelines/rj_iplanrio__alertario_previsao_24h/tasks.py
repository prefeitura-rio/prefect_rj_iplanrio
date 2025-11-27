# -*- coding: utf-8 -*-
"""
Tasks para pipeline AlertaRio Previsão 24h
"""

import hashlib
import uuid
from defusedxml import ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
import requests
from prefect import task

from iplanrio.pipelines_utils.logging import log

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    AlertaRioConstants,
)


@task
def fetch_xml_from_url(url: str = AlertaRioConstants.XML_URL.value) -> str:
    """
    Baixa o XML de previsão do AlertaRio

    Args:
        url: URL do XML de previsão

    Returns:
        String com o conteúdo XML
    """
    log(f"Buscando XML de previsão do AlertaRio: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        log("XML baixado com sucesso")
        return response.text

    except requests.exceptions.RequestException as e:
        log(f"Erro ao buscar XML: {e}")
        raise


@task
def parse_xml_to_dict(xml_content: str) -> Dict[str, Any]:
    """
    Faz parsing do XML e retorna estrutura de dados organizada

    Args:
        xml_content: String com o conteúdo XML

    Returns:
        Dict com os dados estruturados do XML
    """
    log("Fazendo parsing do XML")

    try:
        root = ET.fromstring(xml_content)

        # Extrair data de criação
        create_date_str = root.get("Createdate")
        if create_date_str:
            try:
                create_date = datetime.fromisoformat(create_date_str)
            except (ValueError, TypeError) as e:
                log(f"AVISO: Createdate inválido '{create_date_str}': {e}. Usando datetime.now(timezone.utc) como fallback.")
                create_date = datetime.now(timezone.utc)
        else:
            log(f"AVISO: Createdate ausente no XML. Usando datetime.now(timezone.utc) como fallback.")
            create_date = datetime.now(timezone.utc)

        # Extrair previsões meteorológicas
        previsoes = []
        for previsao in root.findall("previsao"):
            previsoes.append({
                "ceu": previsao.get("ceu"),
                "condicaoIcon": previsao.get("condicaoIcon"),
                "datePeriodo": previsao.get("datePeriodo"),
                "dirVento": previsao.get("dirVento"),
                "periodo": previsao.get("periodo"),
                "precipitacao": previsao.get("precipitacao"),
                "temperatura": previsao.get("temperatura"),
                "velVento": previsao.get("velVento"),
            })

        # Extrair quadro sinótico
        quadro_sinotico_elem = root.find("quadroSinotico")
        if quadro_sinotico_elem is not None:
            quadro_sinotico = (
                quadro_sinotico_elem.get("sinotico")
                or (quadro_sinotico_elem.text or "")
            )
        else:
            quadro_sinotico = ""

        # Extrair temperaturas por zona
        temperaturas = []
        temperatura_elem = root.find("Temperatura")
        if temperatura_elem is not None:
            # HARDCODE: usar data do create_date, ignorar data do XML
            data_temp = create_date.date().isoformat()
            for zona in temperatura_elem.findall("Zona"):
                temperaturas.append({
                    "zona": zona.get("zona"),
                    "temp_maxima": float(zona.get("maxima")) if zona.get("maxima") else None,
                    "temp_minima": float(zona.get("minima")) if zona.get("minima") else None,
                    "data": data_temp,
                })

        # Extrair tábua de marés
        mares = []
        tabuas_mares_elem = root.find("TabuasMares")
        if tabuas_mares_elem is not None:
            for tabua in tabuas_mares_elem.findall("tabua"):
                mares.append({
                    "elevacao": tabua.get("elevacao"),
                    "horario": tabua.get("date"),
                    "altura": float(tabua.get("altura")) if tabua.get("altura") else None,
                })

        # Gerar UUID único para esta execução
        id_execucao = str(uuid.uuid4())

        parsed_data = {
            "id_execucao": id_execucao,
            "create_date": create_date,
            "previsoes": previsoes,
            "quadro_sinotico": quadro_sinotico,
            "temperaturas": temperaturas,
            "mares": mares,
        }

        log(f"Parsing concluído: {len(previsoes)} previsões, {len(temperaturas)} zonas, {len(mares)} marés")
        return parsed_data

    except ET.ParseError as e:
        log(f"Erro ao fazer parsing do XML: {e}")
        raise
    except Exception as e:
        log(f"Erro inesperado ao processar XML: {e}")
        raise


@task
def create_dim_quadro_sinotico_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_quadro_sinotico
    Uma única linha por execução contendo o quadro sinótico

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_quadro_sinotico
    """
    log("Criando DataFrame dim_quadro_sinotico")

    id_execucao = parsed_data["id_execucao"]
    create_date = parsed_data["create_date"]
    quadro_sinotico = parsed_data["quadro_sinotico"]

    now = datetime.now()
    data_execucao = now.date()
    hora_execucao = now.time()

    registro = {
        "id_execucao": id_execucao,
        "data_execucao": data_execucao,
        "hora_execucao": hora_execucao,
        "sinotico": quadro_sinotico,
        "data_alertario": create_date,
        "data_particao": data_execucao,
    }

    df = pd.DataFrame([registro])
    log(f"DataFrame dim_quadro_sinotico criado com {len(df)} registro")

    return df


@task
def create_dim_previsao_periodo_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_previsao_periodo
    Cada linha = uma previsão do XML

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_previsao_periodo
    """
    log("Criando DataFrame dim_previsao_periodo")

    id_execucao = parsed_data["id_execucao"]
    create_date = parsed_data["create_date"]
    previsoes = parsed_data["previsoes"]

    now = datetime.now()
    data_execucao = now.date()
    hora_execucao = now.time()

    registros = []
    for prev in previsoes:
        registros.append({
            "id_execucao": id_execucao,
            "data_execucao": data_execucao,
            "hora_execucao": hora_execucao,
            "data_periodo": datetime.strptime(prev["datePeriodo"], "%Y-%m-%d").date(),
            "periodo": prev["periodo"],
            "ceu": prev["ceu"],
            "precipitacao": prev["precipitacao"],
            "dir_vento": prev["dirVento"],
            "vel_vento": prev["velVento"],
            "temperatura": prev["temperatura"],
            "condicao_icon": prev["condicaoIcon"],
            "data_alertario": create_date,
            "data_particao": data_execucao,
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_previsao_periodo criado com {len(df)} registros")

    return df


@task
def create_dim_temperatura_zona_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_temperatura_zona
    Cada linha = uma zona do XML

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_temperatura_zona
    """
    log("Criando DataFrame dim_temperatura_zona")

    id_execucao = parsed_data["id_execucao"]
    create_date = parsed_data["create_date"]
    temperaturas = parsed_data["temperaturas"]

    now = datetime.now()
    data_execucao = now.date()
    hora_execucao = now.time()

    registros = []
    for temp in temperaturas:
        registros.append({
            "id_execucao": id_execucao,
            "data_execucao": data_execucao,
            "hora_execucao": hora_execucao,
            "zona": temp["zona"],
            "temp_minima": temp["temp_minima"],
            "temp_maxima": temp["temp_maxima"],
            "data_alertario": create_date,
            "data_particao": data_execucao,
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_temperatura_zona criado com {len(df)} registros")

    return df


@task
def create_dim_mares_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_mares
    Cada linha = uma tábua de maré do XML

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_mares
    """
    log("Criando DataFrame dim_mares")

    id_execucao = parsed_data["id_execucao"]
    create_date = parsed_data["create_date"]
    mares = parsed_data["mares"]

    now = datetime.now()
    data_execucao = now.date()
    hora_execucao = now.time()

    registros = []
    for mare in mares:
        # Parse do horário da maré
        horario_str = mare.get("horario")
        if horario_str:
            try:
                # Primeiro tenta ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
                data_hora = datetime.fromisoformat(horario_str)
            except ValueError:
                try:
                    # Fallback para formato com espaço (YYYY-MM-DD HH:MM:SS)
                    data_hora = datetime.strptime(horario_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        # Fallback para formato brasileiro (DD/MM/YYYY HH:MM:SS)
                        data_hora = datetime.strptime(horario_str, "%d/%m/%Y %H:%M:%S")
                    except ValueError:
                        log(f"Formato de horário não reconhecido: {horario_str}")
                        data_hora = None
        else:
            data_hora = None

        registros.append({
            "id_execucao": id_execucao,
            "data_execucao": data_execucao,
            "hora_execucao": hora_execucao,
            "data_hora": data_hora,
            "elevacao": mare["elevacao"],
            "altura": mare["altura"],
            "data_alertario": create_date,
            "data_particao": data_execucao,
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_mares criado com {len(df)} registros")

    return df
