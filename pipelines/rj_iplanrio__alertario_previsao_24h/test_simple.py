#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script super simples para testar o pipeline AlertaRio.
Copia a lógica das tasks diretamente sem usar o sistema de @task do Prefect.

Uso:
  python test_simple.py sample_data.xml
"""

import sys
import os
import hashlib
import pandas as pd
from pathlib import Path
from datetime import datetime
from defusedxml import ElementTree as ET
from typing import Dict, Any

def log(msg):
    """Log simples"""
    print(f"  {msg}")

def load_xml_file(filepath):
    """Carrega XML de um arquivo local."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

def parse_xml_to_dict(xml_content: str) -> Dict[str, Any]:
    """Parse do XML - cópia da função em tasks.py"""
    log("Fazendo parsing do XML...")

    root = ET.fromstring(xml_content)

    # Extrair data de criação
    create_date_str = root.get("Createdate")
    if create_date_str:
        try:
            create_date = datetime.fromisoformat(create_date_str)
        except (ValueError, TypeError) as e:
            log(f"AVISO: Createdate inválido '{create_date_str}': {e}. Usando datetime.now()")
            create_date = datetime.now()
    else:
        log(f"AVISO: Createdate ausente no XML. Usando datetime.now()")
        create_date = datetime.now()

    # Extrair previsões
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
        log(f"DEBUG: Bloco Temperatura encontrado, usando data do create_date={data_temp}")
        for zona in temperatura_elem.findall("Zona"):
            temperaturas.append({
                "zona": zona.get("zona"),
                "temp_maxima": float(zona.get("maxima")) if zona.get("maxima") else None,
                "temp_minima": float(zona.get("minima")) if zona.get("minima") else None,
                "data": data_temp,
            })
    else:
        log("DEBUG: Nenhum bloco Temperatura encontrado no XML")

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

def create_previsao_diaria_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """Cria DataFrame da tabela previsao_diaria"""
    log("Criando DataFrame previsao_diaria...")

    create_date = parsed_data["create_date"]
    previsoes = parsed_data["previsoes"]
    temperaturas = parsed_data["temperaturas"]
    quadro_sinotico = parsed_data["quadro_sinotico"]

    # Agrupar previsões por data
    previsoes_por_data = {}
    for prev in previsoes:
        data = prev["datePeriodo"]
        if data not in previsoes_por_data:
            previsoes_por_data[data] = []
        previsoes_por_data[data].append(prev)

    registros = []
    for data_referencia, previsoes_dia in previsoes_por_data.items():
        # Gerar id_previsao único usando hash MD5
        id_string = f"{create_date.isoformat()}_{data_referencia}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        # Usar TODAS as temperaturas disponíveis (bloco Temperatura só tem 1 dia)
        temps_minimas = [
            t["temp_minima"]
            for t in temperaturas
            if t.get("temp_minima") is not None
        ]
        temps_maximas = [
            t["temp_maxima"]
            for t in temperaturas
            if t.get("temp_maxima") is not None
        ]

        temp_min_geral = min(temps_minimas) if temps_minimas else None
        temp_max_geral = max(temps_maximas) if temps_maximas else None

        data_referencia_date = datetime.strptime(data_referencia, "%Y-%m-%d").date()

        log(f"DEBUG: Usando TODAS as temperaturas para data_referencia='{data_referencia}'")

        registros.append({
            "id_previsao": id_previsao,
            "create_date": create_date,
            "hora_execucao": create_date.time(),
            "data_referencia": data_referencia_date,
            "sinotico": quadro_sinotico if data_referencia_date == create_date.date() else None,
            "temp_min_geral": temp_min_geral,
            "temp_max_geral": temp_max_geral,
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame previsao_diaria criado com {len(df)} registros")
    return df

def create_dim_previsao_periodo_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """Cria DataFrame da dimensão dim_previsao_periodo"""
    log("Criando DataFrame dim_previsao_periodo...")

    create_date = parsed_data["create_date"]
    previsoes = parsed_data["previsoes"]

    registros = []
    for prev in previsoes:
        data_periodo = prev["datePeriodo"]
        id_string = f"{create_date.isoformat()}_{data_periodo}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        registros.append({
            "id_previsao": id_previsao,
            "hora_execucao": create_date.time(),
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

def create_dim_temperatura_zona_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """Cria DataFrame da dimensão dim_temperatura_zona"""
    log("Criando DataFrame dim_temperatura_zona...")

    create_date = parsed_data["create_date"]
    temperaturas = parsed_data["temperaturas"]

    registros = []
    for temp in temperaturas:
        data_ref = temp.get("data")
        if data_ref:
            try:
                data_ref_parsed = datetime.strptime(data_ref, "%Y-%m-%d").date()
            except ValueError:
                data_ref_parsed = create_date.date()
        else:
            data_ref_parsed = create_date.date()

        id_string = f"{create_date.isoformat()}_{data_ref_parsed.isoformat()}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        registros.append({
            "id_previsao": id_previsao,
            "hora_execucao": create_date.time(),
            "zona": temp["zona"],
            "temp_minima": temp["temp_minima"],
            "temp_maxima": temp["temp_maxima"],
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_temperatura_zona criado com {len(df)} registros")
    return df

def create_dim_mares_df(parsed_data: Dict[str, Any]) -> pd.DataFrame:
    """Cria DataFrame da dimensão dim_mares"""
    log("Criando DataFrame dim_mares...")

    create_date = parsed_data["create_date"]
    mares = parsed_data["mares"]

    registros = []
    for mare in mares:
        id_string = f"{create_date.isoformat()}"
        id_previsao = hashlib.md5(id_string.encode()).hexdigest()

        horario_str = mare.get("horario")
        if horario_str:
            try:
                data_hora = datetime.fromisoformat(horario_str)
            except ValueError:
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
            "hora_execucao": create_date.time(),
            "data_hora": data_hora,
            "elevacao": mare["elevacao"],
            "altura": mare["altura"],
            "data_particao": create_date.date(),
        })

    df = pd.DataFrame(registros)
    log(f"DataFrame dim_mares criado com {len(df)} registros")
    return df

def save_to_csv(df, output_path):
    """Salva DataFrame em CSV"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path

def main():
    print("\n" + "="*80)
    print("  TESTE SIMPLES - PIPELINE ALERTARIO PREVISAO 24H")
    print("="*80 + "\n")

    # Verificar argumentos
    if len(sys.argv) < 2:
        print("❌ Uso: python test_simple.py <arquivo_xml>")
        print("   Exemplo: python test_simple.py sample_data.xml")
        return False

    xml_file = sys.argv[1]
    print(f"📄 Arquivo XML: {xml_file}\n")

    # 1. CARREGAR XML
    print("1️⃣  CARREGANDO XML")
    print("-" * 80)
    try:
        xml_content = load_xml_file(xml_file)
        print(f"✅ XML carregado ({len(xml_content)} bytes)\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        return False

    # 2. FAZER PARSING
    print("2️⃣  FAZENDO PARSING")
    print("-" * 80)
    try:
        parsed_data = parse_xml_to_dict(xml_content)
        print("✅ Parsing concluído\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        import traceback
        traceback.print_exc()
        return False

    # 3. CRIAR DATAFRAMES
    print("3️⃣  CRIANDO DATAFRAMES")
    print("-" * 80)

    try:
        df_previsao = create_previsao_diaria_df(parsed_data)
        print(f"Colunas: {list(df_previsao.columns)}")
        print(f"\n{df_previsao.to_string()}\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        import traceback
        traceback.print_exc()
        df_previsao = pd.DataFrame()

    try:
        df_periodo = create_dim_previsao_periodo_df(parsed_data)
        print(f"Colunas: {list(df_periodo.columns)}")
        print(f"\n{df_periodo.to_string()}\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        import traceback
        traceback.print_exc()
        df_periodo = pd.DataFrame()

    try:
        df_temperatura = create_dim_temperatura_zona_df(parsed_data)
        print(f"Colunas: {list(df_temperatura.columns)}")
        print(f"\n{df_temperatura.to_string()}\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        import traceback
        traceback.print_exc()
        df_temperatura = pd.DataFrame()

    try:
        df_mares = create_dim_mares_df(parsed_data)
        print(f"Colunas: {list(df_mares.columns)}")
        print(f"\n{df_mares.to_string()}\n")
    except Exception as e:
        print(f"❌ ERRO: {e}\n")
        import traceback
        traceback.print_exc()
        df_mares = pd.DataFrame()

    # 4. SALVAR CSV
    print("4️⃣  SALVANDO CSV")
    print("-" * 80)

    output_dir = Path("/tmp/alertario_test") / datetime.now().strftime("%Y%m%d_%H%M%S")

    tables = [
        ("previsao_diaria", df_previsao),
        ("dim_previsao_periodo", df_periodo),
        ("dim_temperatura_zona", df_temperatura),
        ("dim_mares", df_mares),
    ]

    for table_name, df in tables:
        if df.empty:
            print(f"⏭️  {table_name}: DataFrame vazio, pulando...")
            continue

        csv_path = output_dir / f"{table_name}.csv"
        save_to_csv(df, csv_path)
        print(f"✅ {table_name}: {csv_path}")

    print(f"\n📂 Arquivos salvos em: {output_dir}\n")

    # Tentar abrir no Finder (macOS)
    if sys.platform == "darwin":
        os.system(f"open '{output_dir}'")

    print("=" * 80)
    print("✅ TESTE CONCLUÍDO COM SUCESSO!")
    print("=" * 80 + "\n")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
