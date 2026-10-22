#!/usr/bin/env python3
"""
Script local para testar o pipeline AlertaRio sem fazer upload para BigQuery.
Testa: XML parsing, DataFrame creation, CSV generation.

Uso:
  python test_local.py                    # Busca XML do AlertaRio (pode ser lento)
  python test_local.py --file <caminho>  # Usa XML de um arquivo local
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import argparse

# Adicionar o diretório do projeto ao path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Importar as tasks
from pipelines.rj_iplanrio__alertario_previsao_24h.tasks import (
    fetch_xml_from_url,
    parse_xml_to_dict,
    create_previsao_diaria_df,
    create_dim_previsao_periodo_df,
    create_dim_temperatura_zona_df,
    create_dim_mares_df,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.helpers.tasks import (
    create_date_partitions,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    AlertaRioConstants,
)


def print_section(title):
    """Imprime um separador de seção."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def load_xml_from_file(filepath):
    """Carrega XML de um arquivo local."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def test_pipeline(xml_file=None):
    """Executa o pipeline de teste localmente."""

    print_section("TESTE LOCAL - PIPELINE ALERTARIO PREVISAO 24H")

    # ============================================================================
    # 1. BUSCAR XML
    # ============================================================================
    print_section("1. BUSCANDO/CARREGANDO XML DO ALERTARIO")
    try:
        if xml_file:
            print(f"Carregando XML de arquivo local: {xml_file}")
            xml_content = load_xml_from_file(xml_file)
            print(f"✅ XML carregado de arquivo ({len(xml_content)} bytes)")
        else:
            print("Buscando XML da URL...")
            xml_content = fetch_xml_from_url()
            print(f"✅ XML baixado com sucesso ({len(xml_content)} bytes)")
        print(f"Primeiros 200 caracteres:\n{xml_content[:200]}...")
    except Exception as e:
        print(f"❌ ERRO ao buscar/carregar XML: {e}")
        return False

    # ============================================================================
    # 2. FAZER PARSING
    # ============================================================================
    print_section("2. FAZENDO PARSING DO XML")
    try:
        parsed_data = parse_xml_to_dict(xml_content)
        print("✅ Parsing concluído com sucesso")
        print(f"   - Previsões: {len(parsed_data['previsoes'])}")
        print(f"   - Temperaturas: {len(parsed_data['temperaturas'])}")
        print(f"   - Marés: {len(parsed_data['mares'])}")
        print(f"   - Create date: {parsed_data['create_date']}")
        print(f"   - Quadro sinótico: {len(parsed_data['quadro_sinotico'])} chars")
    except Exception as e:
        print(f"❌ ERRO ao fazer parsing: {e}")
        import traceback
        traceback.print_exc()
        return False

    # ============================================================================
    # 3. CRIAR DATAFRAMES
    # ============================================================================
    print_section("3. CRIANDO DATAFRAMES")

    try:
        # Tabela 1: previsao_diaria
        df_previsao_diaria = create_previsao_diaria_df(parsed_data)
        print(f"✅ previsao_diaria: {len(df_previsao_diaria)} registros")
        if len(df_previsao_diaria) > 0:
            print(f"   Colunas: {list(df_previsao_diaria.columns)}")
            print(f"   Primeiras linhas:\n{df_previsao_diaria.head()}\n")
        else:
            print("   ⚠️  DataFrame vazio!")
    except Exception as e:
        print(f"❌ ERRO ao criar previsao_diaria: {e}")
        import traceback
        traceback.print_exc()
        df_previsao_diaria = None

    try:
        # Tabela 2: dim_previsao_periodo
        df_dim_periodo = create_dim_previsao_periodo_df(parsed_data)
        print(f"✅ dim_previsao_periodo: {len(df_dim_periodo)} registros")
        if len(df_dim_periodo) > 0:
            print(f"   Colunas: {list(df_dim_periodo.columns)}")
            print(f"   Primeiras linhas:\n{df_dim_periodo.head()}\n")
    except Exception as e:
        print(f"❌ ERRO ao criar dim_previsao_periodo: {e}")
        import traceback
        traceback.print_exc()
        df_dim_periodo = None

    try:
        # Tabela 3: dim_temperatura_zona
        df_dim_temperatura = create_dim_temperatura_zona_df(parsed_data)
        print(f"✅ dim_temperatura_zona: {len(df_dim_temperatura)} registros")
        if len(df_dim_temperatura) > 0:
            print(f"   Colunas: {list(df_dim_temperatura.columns)}")
            print(f"   Primeiras linhas:\n{df_dim_temperatura.head()}\n")
    except Exception as e:
        print(f"❌ ERRO ao criar dim_temperatura_zona: {e}")
        import traceback
        traceback.print_exc()
        df_dim_temperatura = None

    try:
        # Tabela 4: dim_mares
        df_dim_mares = create_dim_mares_df(parsed_data)
        print(f"✅ dim_mares: {len(df_dim_mares)} registros")
        if len(df_dim_mares) > 0:
            print(f"   Colunas: {list(df_dim_mares.columns)}")
            print(f"   Primeiras linhas:\n{df_dim_mares.head()}\n")
    except Exception as e:
        print(f"❌ ERRO ao criar dim_mares: {e}")
        import traceback
        traceback.print_exc()
        df_dim_mares = None

    # ============================================================================
    # 4. CRIAR PARTIÇÕES LOCAIS
    # ============================================================================
    print_section("4. CRIANDO PARTIÇÕES LOCAIS (CSV)")

    # Diretório de saída
    output_dir = Path("/tmp/alertario_test") / datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de saída: {output_dir}\n")

    dataframes = [
        ("previsao_diaria", df_previsao_diaria),
        ("dim_previsao_periodo", df_dim_periodo),
        ("dim_temperatura_zona", df_dim_temperatura),
        ("dim_mares", df_dim_mares),
    ]

    results = {}
    for table_name, df in dataframes:
        try:
            if df is None:
                print(f"⏭️  {table_name}: DataFrame é None, pulando...")
                continue

            if df.empty:
                print(f"⚠️  {table_name}: DataFrame vazio")
                # Tentar criar partições mesmo vazio (para testar a função)
                root_folder = str(output_dir / table_name)
                result = create_date_partitions(
                    dataframe=df,
                    partition_column="data_particao",
                    file_format="csv",
                    root_folder=root_folder,
                )
                print(f"   Resultado: {result}")
                results[table_name] = result
                continue

            root_folder = str(output_dir / table_name)
            result = create_date_partitions(
                dataframe=df,
                partition_column="data_particao",
                file_format="csv",
                root_folder=root_folder,
            )

            print(f"✅ {table_name}: Partições criadas")
            print(f"   Localização: {result}")

            # Listar arquivos criados
            result_path = Path(result)
            if result_path.exists():
                csv_files = list(result_path.rglob("*.csv"))
                print(f"   Arquivos CSV: {len(csv_files)}")
                for csv_file in csv_files:
                    size = csv_file.stat().st_size
                    print(f"     - {csv_file.relative_to(output_dir)}: {size} bytes")

            results[table_name] = result

        except Exception as e:
            print(f"❌ ERRO ao criar partições para {table_name}: {e}")
            import traceback
            traceback.print_exc()

    # ============================================================================
    # 5. RESUMO FINAL
    # ============================================================================
    print_section("5. RESUMO FINAL")

    print(f"Diretório de saída: {output_dir}")
    print(f"\nDataFrames criados:")
    print(f"  - previsao_diaria: {len(df_previsao_diaria) if df_previsao_diaria is not None else 'N/A'} registros")
    print(f"  - dim_previsao_periodo: {len(df_dim_periodo) if df_dim_periodo is not None else 'N/A'} registros")
    print(f"  - dim_temperatura_zona: {len(df_dim_temperatura) if df_dim_temperatura is not None else 'N/A'} registros")
    print(f"  - dim_mares: {len(df_dim_mares) if df_dim_mares is not None else 'N/A'} registros")

    print(f"\nPartições criadas: {len(results)} tabelas")
    for table_name, path in results.items():
        print(f"  - {table_name}: {path}")

    # Tentar abrir o diretório no Finder (macOS)
    if sys.platform == "darwin":
        os.system(f"open '{output_dir}'")
        print(f"\n📂 Diretório aberto no Finder")

    print("\n✅ TESTE COMPLETADO COM SUCESSO!\n")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Teste local do pipeline AlertaRio (sem upload para BigQuery)"
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="Caminho para arquivo XML local (ao invés de buscar da URL)",
    )

    args = parser.parse_args()

    try:
        success = test_pipeline(xml_file=args.file)
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ ERRO NÃO TRATADO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
