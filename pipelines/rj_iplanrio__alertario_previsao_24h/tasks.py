# -*- coding: utf-8 -*-
"""
Tasks para pipeline AlertaRio Previsão 24h
"""

import hashlib
from defusedxml import ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List

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
        create_date = datetime.fromisoformat(create_date_str)

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
        quadro_sinotico = quadro_sinotico_elem.text if quadro_sinotico_elem is not None else ""

        # Extrair temperaturas por zona
        temperaturas = []
        temperatura_elem = root.find("Temperatura")
        if temperatura_elem is not None:
            data_temp = temperatura_elem.get("data")
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

        parsed_data = {
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
def create_previsao_diaria_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da tabela fato previsao_diaria com agregações

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela previsao_diaria
    """
    log("Criando DataFrame previsao_diaria")

    create_date = parsed_data["create_date"]
    previsoes = parsed_data["previsoes"]
    temperaturas = parsed_data["temperaturas"]
    quadro_sinotico = parsed_data["quadro_sinotico"]

    # Agrupar previsões por data para calcular agregações
    previsoes_por_data = {}
    for prev in previsoes:
        data = prev["datePeriodo"]
        if data not in previsoes_por_data:
            previsoes_por_data[data] = []
        previsoes_por_data[data].append(prev)

    # Criar registros para cada data
    registros = []
    for data_referencia, previsoes_dia in previsoes_por_data.items():
        # Gerar id_previsao único usando hash MD5
        id_string = f"{create_date.isoformat()}_{data_referencia}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        # Calcular temp_min_geral e temp_max_geral (min/max de todas as zonas)
        temps_minimas = [t["temp_minima"] for t in temperaturas if t["temp_minima"] is not None]
        temps_maximas = [t["temp_maxima"] for t in temperaturas if t["temp_maxima"] is not None]

        temp_min_geral = min(temps_minimas) if temps_minimas else None
        temp_max_geral = max(temps_maximas) if temps_maximas else None

        # Calcular teve_chuva (TRUE se qualquer período tem precipitação não vazia/não None)
        teve_chuva = any(
            prev.get("precipitacao") and prev.get("precipitacao").strip()
            for prev in previsoes_dia
        )

        registros.append({
            "id_previsao": id_previsao,
            "create_date": create_date,
            "data_referencia": datetime.strptime(data_referencia, "%Y-%m-%d").date(),
            "sinotico": quadro_sinotico,
            "temp_min_geral": temp_min_geral,
            "temp_max_geral": temp_max_geral,
            "teve_chuva": teve_chuva,
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame previsao_diaria criado com {len(df)} registros")

    return df


@task
def create_dim_previsao_periodo_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_previsao_periodo

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_previsao_periodo
    """
    log("Criando DataFrame dim_previsao_periodo")

    create_date = parsed_data["create_date"]
    previsoes = parsed_data["previsoes"]

    registros = []
    for prev in previsoes:
        data_periodo = prev["datePeriodo"]

        # Gerar id_previsao usando hash MD5 (mesmo da tabela fato)
        id_string = f"{create_date.isoformat()}_{data_periodo}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        registros.append({
            "id_previsao": id_previsao,
            "data_periodo": datetime.strptime(data_periodo, "%Y-%m-%d").date(),
            "periodo": prev["periodo"],
            "ceu": prev["ceu"],
            "precipitacao": prev["precipitacao"],
            "dir_vento": prev["dirVento"],
            "vel_vento": prev["velVento"],
            "temperatura": prev["temperatura"],
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_previsao_periodo criado com {len(df)} registros")

    return df


@task
def create_dim_temperatura_zona_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_temperatura_zona

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_temperatura_zona
    """
    log("Criando DataFrame dim_temperatura_zona")

    create_date = parsed_data["create_date"]
    temperaturas = parsed_data["temperaturas"]

    registros = []
    for temp in temperaturas:
        # Para temperatura, usar a data de referência da própria temperatura (se disponível)
        # ou usar a data de criação
        data_ref = temp.get("data")
        if data_ref:
            try:
                data_ref_parsed = datetime.strptime(data_ref, "%Y-%m-%d").date()
            except ValueError:
                data_ref_parsed = create_date.date()
        else:
            data_ref_parsed = create_date.date()

        # Gerar id_previsao usando hash MD5
        id_string = f"{create_date.isoformat()}_{data_ref_parsed.isoformat()}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        registros.append({
            "id_previsao": id_previsao,
            "zona": temp["zona"],
            "temp_minima": temp["temp_minima"],
            "temp_maxima": temp["temp_maxima"],
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_temperatura_zona criado com {len(df)} registros")

    return df


@task
def create_dim_mares_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Cria DataFrame da dimensão dim_mares

    Args:
        parsed_data: Dados estruturados do XML

    Returns:
        DataFrame com dados da tabela dim_mares
    """
    log("Criando DataFrame dim_mares")

    create_date = parsed_data["create_date"]
    mares = parsed_data["mares"]

    registros = []
    for mare in mares:
        # Gerar id_previsao baseado na data de criação
        id_string = f"{create_date.isoformat()}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        # Parse do horário da maré
        horario_str = mare.get("horario")
        if horario_str:
            try:
                data_hora = datetime.strptime(horario_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    data_hora = datetime.strptime(horario_str, "%d/%m/%Y %H:%M:%S")
                except ValueError:
                    log(f"Formato de horário não reconhecido: {horario_str}")
                    data_hora = None
        else:
            data_hora = None

        registros.append({
            "id_previsao": id_previsao,
            "data_hora": data_hora,
            "elevacao": mare["elevacao"],
            "altura": mare["altura"],
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_mares criado com {len(df)} registros")

    return df
