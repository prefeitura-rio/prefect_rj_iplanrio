# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados de precipitação e fluviometria do INEA.
"""

from io import BytesIO
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pendulum
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_cor__precipitacao_inea.utils import (
    parse_date_columns,
    to_partitions,
)


@task(retries=3, retry_delay_seconds=10)
def download_inea_data_task() -> pd.DataFrame:
    """
    Faz o download dos dados de precipitação e fluviometria do INEA.

    O INEA (Instituto Estadual do Ambiente) disponibiliza dados de 5 estações
    meteorológicas do Sistema de Alerta de Cheias do Rio de Janeiro via arquivos
    Excel acessíveis por HTTP.

    Cada estação possui um código e fornece dados de:
    - Precipitação (acumulados de chuva)
    - Fluviometria (altura da água em rios)

    Returns:
        DataFrame consolidado com dados de todas as estações.

    Raises:
        requests.HTTPError: Se a requisição HTTP falhar.
        requests.RequestException: Se houver erro de conexão.

    Examples:
        >>> dfr = download_inea_data_task()
        >>> print(dfr['id_estacao'].unique())
        [1, 2, 3, 4, 5]

    Notes:
        - 5 estações conhecidas:
          1: Campo Grande
          2: Capela Mayrink
          3: Eletrobras
          4: Realengo
          5: São Cristóvão
        - URL base: f"{url_base}/{value}.xlsx"
        - Formato: Excel (.xlsx)
        - Timeout de 30 segundos por estação
        - Retry automático em caso de falha (3 tentativas)
    """
    # Dicionário de estações INEA
    stations = {
        "1": "225543320",  # Campo Grande
        "2": "BE70E166",  # Capela Mayrink
        "3": "225543250",  # Eletrobras
        "4": "2243088",  # Realengo
        "5": "225443130",  # Sao Cristovao
    }

    http_ok = 200

    dataframes = []

    print("📥 Iniciando download dos dados do INEA...")
    print(f"   Total de estações: {len(stations)}")

    url_base = getenv_or_action("URL-ALERTA-CHEIAS")
    for key, value in stations.items():
        url = f"{url_base}/{value}.xlsx"
        print(f"   Baixando estação {key} - {value}...")
        print(f"   URL: {url}")
        try:
            response = requests.get(url, timeout=30, verify=False)
            if response.status_code == http_ok:
                # Ler arquivo Excel diretamente do conteúdo da resposta
                dfr_station = pd.read_excel(BytesIO(response.content))

                # Adicionar coluna com ID da estação
                dfr_station["id_estacao"] = key

                dataframes.append(dfr_station)


                print(f"      ✅ {len(dfr_station)} registros baixados")


            else:
                print(f"      ❌ Erro HTTP {response.status_code}")
                raise requests.HTTPError(f"Status code: {response.status_code} para estação {key}")

        except requests.RequestException as e:
            print(f"      ❌ Erro ao baixar estação {key}: {e}")
            raise


    # Concatenar todos os DataFrames
    dfr_combined = pd.concat(dataframes, ignore_index=True)

    print(f"\n✅ Download concluído!")
    print(f"   Total de registros: {len(dfr_combined)}")
    print(f"   Estações processadas: {sorted(dfr_combined['id_estacao'].unique().tolist())}")
    return dfr_combined


@task
def transform_inea_data_task(dfr: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforma e separa os dados do INEA em pluviométricos e fluviométricos.

    Realiza as seguintes operações:
    - Combina colunas Data e Hora em data_medicao
    - Renomeia colunas para padrão do projeto
    - Limpa valores "Dado Nulo"
    - Remove duplicatas
    - Separa em dois DataFrames:
      1. Pluviométrico: acumulados de chuva (7 colunas)
      2. Fluviométrico: altura da água

    Args:
        dfr: DataFrame com dados brutos do INEA

    Returns:
        Tupla contendo:
            - DataFrame pluviométrico (precipitação)
            - DataFrame fluviométrico (altura de água)

    Examples:
        >>> dfr_raw = download_inea_data_task()
        >>> dfr_pluv, dfr_fluv = transform_inea_data_task(dfr_raw)
        >>> print(dfr_pluv.columns)
        Index(['id_estacao', 'data_medicao', 'acumulado_chuva_15_min', ...])

    Notes:
        - Dados pluviométricos: 7 acumulados (15min a 96h)
        - Dados fluviométricos: altura da água (filtrado para estações pluviométricas)
        - Valores "Dado Nulo" substituídos por NaN
        - Altura de água > 10000 tratada como erro (removida)
        - Duplicatas removidas por id_estacao + data_medicao
    """
    print("🔄 Iniciando transformação dos dados do INEA...")
    print(f"   Registros iniciais: {len(dfr)}")

    # Combinar colunas Data e Hora em data_medicao
    dfr["data_medicao"] = pd.to_datetime(
        dfr["Data"].astype(str).str.strip() + " " +
        dfr["Hora"].astype(str).str.strip(),
        format="%d/%m/%Y %H:%M",
        errors="coerce",
    )

    # Renomear colunas
    rename_cols = {
        "Chuva Último dado": "acumulado_chuva_15_min",
        " Chuva Acumulada 1H": "acumulado_chuva_1_h",
        " Chuva Acumulada 4H": "acumulado_chuva_4_h",
        " Chuva Acumulada 24H": "acumulado_chuva_24_h",
        " Chuva Acumulada 96H": "acumulado_chuva_96_h",
        " Chuva Acumulada 30D": "acumulado_chuva_30_d",
        " Último Nível": "altura_agua",
    }

    dfr = dfr.rename(columns=rename_cols)

    # Substituir "Dado Nulo" por NaN
    dfr = dfr.replace("Dado Nulo", pd.NA)

    # Remover duplicatas por id_estacao e data_medicao
    before_dedup = len(dfr)
    ()
    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")
    after_dedup = len(dfr)
    if before_dedup > after_dedup:
        print(f"   Removidas {before_dedup - after_dedup} duplicatas")

    # Converter data_medicao para string
    dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # === Criar DataFrame Pluviométrico ===
    pluviometric_cols = [
    pluviometric_cols = [
        "id_estacao",
        "data_medicao",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1_h",
        "acumulado_chuva_4_h",
        "acumulado_chuva_24_h",
        "acumulado_chuva_96_h",
        "acumulado_chuva_30_d",
    ]
    dfr_pluviometric = dfr[[col for col in pluviometric_cols if col in dfr.columns]].copy()

    # Converter colunas numéricas
    for col in pluviometric_cols[2:]:  # Pular id_estacao e data_medicao
        if col in dfr_pluviometric.columns:
            dfr_pluviometric[col] = pd.to_numeric(dfr_pluviometric[col], errors="coerce")

    print(f"\n📊 Dados pluviométricos:")
    print(f"   Registros: {len(dfr_pluviometric)}")
    print(f"   Colunas: {len(dfr_pluviometric.columns)}")

    # === Criar DataFrame Fluviométrico ===
    fluviometric_cols = [
        "id_estacao",
        "data_medicao",
        "altura_agua",
    ]

    dfr_fluviometric = dfr[[col for col in fluviometric_cols if col in dfr.columns]].copy()
    ()
    # Converter altura_agua para numérico
    dfr_fluviometric["altura_agua"] = pd.to_numeric(dfr_fluviometric["altura_agua"], errors="coerce")

    # Remover valores de altura_agua maiores que 10000 (erro de medição)
    before_filter = len(dfr_fluviometric)
    dfr_fluviometric = dfr_fluviometric[
        (dfr_fluviometric["altura_agua"].isna()) | (dfr_fluviometric["altura_agua"] <= 10000)
    ].copy()
    after_filter = len(dfr_fluviometric)
    if before_filter > after_filter:
        print(f"   Removidos {before_filter - after_filter} registros com altura_agua > 10000")

    print(f"\n📊 Dados fluviométricos:")
    print(f"   Registros: {len(dfr_fluviometric)}")
    print(f"   Colunas: {len(dfr_fluviometric.columns)}")

    return dfr_pluviometric, dfr_fluviometric


@task
def save_data_to_partitions_task(
    dfr: pd.DataFrame,
    data_name: str = "temp",
    partition_column: str = "data_medicao",
) -> Path:
    """
    Salva os dados em partições CSV organizadas por data.

    Os dados são particionados pela coluna especificada e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dfr: DataFrame com dados processados
        data_name: Nome do tipo de dado (para organizar o caminho)
        partition_column: Nome da coluna para particionamento

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Examples:
        >>> dfr_pluv, _ = transform_inea_data_task(dfr_raw)
        >>> prepath = save_data_to_partitions_task(dfr_pluv, "pluviometric")
        >>> print(prepath)
        /tmp/precipitacao_inea/pluviometric

    Notes:
        - Formato de partição: ano=YYYY/mes=MM/data=YYYY-MM-DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/precipitacao_inea/
        - Timestamp adicionado ao nome do arquivo para evitar sobrescrita
    """
    prepath = Path(f"/tmp/precipitacao_inea/{data_name}")
    prepath.mkdir(parents=True, exist_ok=True)

    print(f"💾 Salvando dados {data_name} em partições...")
    print(f"   Diretório base: {prepath}")

    if dfr.empty:
        print("⚠️  DataFrame vazio, nenhum arquivo será salvo")
        return prepath

    print(f"   DataFrame: {dfr.shape[0]} registros")

    # Remove colunas de partição se já existirem
    new_partition_columns = ["ano_particao", "mes_particao", "data_particao"]
    dfr = dfr.drop(columns=[col for col in new_partition_columns if col in dfr.columns])

    # Cria partições
    dataframe, partitions = parse_date_columns(dfr, partition_column)

    # Salva em partições
    suffix = pendulum.now("America/Sao_Paulo").strftime("%Y%m%d%H%M")
    to_partitions(
        data=dataframe,
        partition_columns=partitions,
        savepath=prepath,
        data_type="csv",
        suffix=suffix,
    )

    print(f"✅ Arquivos salvos em: {prepath}")

    return prepath


@task
def check_for_new_stations_task(dfr: pd.DataFrame) -> None:
    """
    Verifica se há novas estações não cadastradas no sistema.

    Compara os IDs das estações presentes nos dados com uma lista conhecida
    de estações do INEA. Se detectar novas estações, emite um aviso.

    Args:
        dfr: DataFrame com dados processados contendo coluna 'id_estacao'

    Returns:
        None

    Examples:
        >>> dfr_pluv, _ = transform_inea_data_task(dfr_raw)
        >>> check_for_new_stations_task(dfr_pluv)
        ✅ Nenhuma nova estação detectada

    Notes:
        - Lista de estações conhecidas: 5 estações (1-5)
        - Se novas estações forem detectadas, apenas loga aviso
        - Não interrompe o flow (comportamento diferente do Prefect 1.4)
        - Verificação importante para manutenção do cadastro de estações
    """
    # Lista de estações conhecidas do INEA
    known_stations = [1, 2, 3, 4, 5]

    print("🔍 Verificando novas estações...")
    print(f"   Estações conhecidas: {known_stations}")

    # Obter IDs únicos das estações nos dados
    current_stations = dfr["id_estacao"].unique().tolist()
    print(f"   Estações nos dados: {current_stations}")

    # Verificar se há estações novas
    new_stations = [station for station in current_stations if station not in known_stations]

    if len(new_stations) == 0:
        print("✅ Nenhuma nova estação detectada")
    else:
        print(f"⚠️  Nova(s) estação(ões) identificada(s): {new_stations}")
        print("   Você precisa atualizar o cadastro de estações INEA!")
        print("   Atualize a lista de estações conhecidas em:")
        print("   - tasks.py: função check_for_new_stations_task()")
        print("   - Tabela estacoes_inea no BigQuery (se existir)")
