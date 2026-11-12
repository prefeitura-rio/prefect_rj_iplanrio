#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Teste das 4 tabelas com UUID
"""

import sys
import os
import uuid
from datetime import datetime
from defusedxml import ElementTree as ET
from pathlib import Path
import pandas as pd

def log(msg):
    print(f"  {msg}")

def load_xml_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def parse_xml_to_dict(xml_content: str):
    root = ET.fromstring(xml_content)

    # Extrair data de criação
    create_date_str = root.get("Createdate")
    create_date = datetime.fromisoformat(create_date_str) if create_date_str else datetime.now()

    # Gerar UUID único para esta execução
    id_execucao = str(uuid.uuid4())

    # Extrair previsões
    previsoes = []
    for previsao in root.findall("previsao"):
        previsoes.append({
            "ceu": previsao.get("ceu"),
            "datePeriodo": previsao.get("datePeriodo"),
            "dirVento": previsao.get("dirVento"),
            "periodo": previsao.get("periodo"),
            "precipitacao": previsao.get("precipitacao"),
            "temperatura": previsao.get("temperatura"),
            "velVento": previsao.get("velVento"),
        })

    # Extrair quadro sinótico
    quadro_sinotico_elem = root.find("quadroSinotico")
    quadro_sinotico = quadro_sinotico_elem.get("sinotico") or (quadro_sinotico_elem.text or "") if quadro_sinotico_elem is not None else ""

    # Extrair temperaturas
    temperaturas = []
    temperatura_elem = root.find("Temperatura")
    if temperatura_elem is not None:
        # Extrair data da temperatura do atributo "data" do XML
        data_temperatura_str = temperatura_elem.get("data")
        data_temperatura = data_temperatura_str if data_temperatura_str else create_date.date().isoformat()

        for zona in temperatura_elem.findall("Zona"):
            temperaturas.append({
                "zona": zona.get("zona"),
                "temp_maxima": float(zona.get("maxima")) if zona.get("maxima") else None,
                "temp_minima": float(zona.get("minima")) if zona.get("minima") else None,
                "data_temperatura": data_temperatura,
            })

    # Extrair marés
    mares = []
    tabuas_mares_elem = root.find("TabuasMares")
    if tabuas_mares_elem is not None:
        for tabua in tabuas_mares_elem.findall("tabua"):
            mares.append({
                "elevacao": tabua.get("elevacao"),
                "horario": tabua.get("date"),
                "altura": float(tabua.get("altura")) if tabua.get("altura") else None,
            })

    return {
        "id_execucao": id_execucao,
        "create_date": create_date,
        "previsoes": previsoes,
        "quadro_sinotico": quadro_sinotico,
        "temperaturas": temperaturas,
        "mares": mares,
    }

def create_dim_quadro_sinotico_df(parsed_data):
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

def create_dim_previsao_periodo_df(parsed_data):
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
            "data_alertario": create_date,
            "data_particao": data_execucao,
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_previsao_periodo criado com {len(df)} registros")

    return df

def create_dim_temperatura_zona_df(parsed_data):
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
            "data_temperatura": temp["data_temperatura"],
            "data_alertario": create_date,
            "data_particao": data_execucao,
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_temperatura_zona criado com {len(df)} registros")

    return df

def create_dim_mares_df(parsed_data):
    log("Criando DataFrame dim_mares")

    id_execucao = parsed_data["id_execucao"]
    create_date = parsed_data["create_date"]
    mares = parsed_data["mares"]

    now = datetime.now()
    data_execucao = now.date()
    hora_execucao = now.time()

    registros = []
    for mare in mares:
        horario_str = mare.get("horario")
        try:
            data_hora = datetime.fromisoformat(horario_str) if horario_str else None
        except:
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

def save_to_csv(df, output_path):
    """Salva DataFrame em CSV"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path

if __name__ == "__main__":
    xml_file = sys.argv[1] if len(sys.argv) > 1 else "sample_data.xml"

    print("\n" + "="*80)
    print("  TESTE - 4 TABELAS COM UUID")
    print("="*80 + "\n")

    xml_content = load_xml_file(xml_file)
    parsed_data = parse_xml_to_dict(xml_content)

    print(f"UUID da execução: {parsed_data['id_execucao']}\n")

    df_sinotico = create_dim_quadro_sinotico_df(parsed_data)
    df_periodo = create_dim_previsao_periodo_df(parsed_data)
    df_temp = create_dim_temperatura_zona_df(parsed_data)
    df_mares = create_dim_mares_df(parsed_data)

    print("=== dim_quadro_sinotico ===")
    print(f"Colunas: {list(df_sinotico.columns)}")
    print(f"Linhas: {len(df_sinotico)}")
    print(df_sinotico.to_string())
    print()

    print("=== dim_previsao_periodo ===")
    print(f"Colunas: {list(df_periodo.columns)}")
    print(f"Linhas: {len(df_periodo)}")
    print(df_periodo.to_string())
    print()

    print("=== dim_temperatura_zona ===")
    print(f"Colunas: {list(df_temp.columns)}")
    print(f"Linhas: {len(df_temp)}")
    print(df_temp.to_string())
    print()

    print("=== dim_mares ===")
    print(f"Colunas: {list(df_mares.columns)}")
    print(f"Linhas: {len(df_mares)}")
    print(df_mares.to_string())
    print()

    # Salvar CSVs
    output_dir = Path("/tmp/alertario_test") / datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_sinotico = save_to_csv(df_sinotico, output_dir / "dim_quadro_sinotico.csv")
    csv_periodo = save_to_csv(df_periodo, output_dir / "dim_previsao_periodo.csv")
    csv_temp = save_to_csv(df_temp, output_dir / "dim_temperatura_zona.csv")
    csv_mares = save_to_csv(df_mares, output_dir / "dim_mares.csv")

    print("="*80)
    print("ARQUIVOS SALVOS:")
    print(f"  1. {csv_sinotico}")
    print(f"  2. {csv_periodo}")
    print(f"  3. {csv_temp}")
    print(f"  4. {csv_mares}")
    print("="*80 + "\n")

    # Tentar abrir no Finder (macOS)
    if sys.platform == "darwin":
        os.system(f"open '{output_dir}'")

    print("✅ TESTE CONCLUÍDO!")
    print(f"📂 Diretório: {output_dir}\n")
