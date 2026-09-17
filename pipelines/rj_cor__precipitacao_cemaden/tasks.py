# -*- coding: utf-8 -*-
"""
Tasks para coleta e processamento de dados de precipitação do CEMADEN.
"""

from pathlib import Path
from typing import List

import pandas as pd
import pendulum
import requests
from prefect import task

from pipelines.rj_cor__precipitacao_cemaden.utils import (
    parse_date_columns,
    to_partitions,
)


@task(retries=3, retry_delay_seconds=10)
def download_cemaden_data_task() -> pd.DataFrame:
    """
    Faz o download dos dados de precipitação do CEMADEN.

    O CEMADEN (Centro Nacional de Monitoramento e Alertas de Desastres Naturais)
    disponibiliza dados de precipitação de estações pluviométricas distribuídas
    pelo Brasil via API JSON.

    Returns:
        DataFrame com dados brutos de precipitação do CEMADEN.

    Raises:
        requests.HTTPError: Se a requisição HTTP falhar.
        requests.RequestException: Se houver erro de conexão.

    Examples:
        >>> dfr = download_cemaden_data_task()
        >>> print(dfr.columns)
        Index(['idestacao', 'uf', 'codibge', 'cidade', 'nomeestacao', ...])

    Notes:
        - A API retorna dados de todas as estações do estado (UF=RJ)
        - Dados vêm em formato JSON
        - Inclui informações de localização e tipo de estação
        - Timeout de 30 segundos configurado
        - Retry automático em caso de falha (3 tentativas)
    """
    url = "http://sjc.salvar.cemaden.gov.br/resources/graficos/interativo/getJson2.php?uf=RJ"
    http_ok = 200

    try:
        print(f"📡 Fazendo requisição para API do CEMADEN: {url}")
        response = requests.get(url, timeout=30)

        if response.status_code == http_ok:
            print("✅ Dados recebidos com sucesso")

            # Converter JSON para DataFrame
            dfr = pd.DataFrame(response.json())

            print(f"📊 Total de registros baixados: {len(dfr)}")
            print(f"   Colunas: {', '.join(dfr.columns.tolist())}")

            return dfr
        else:
            print(f"❌ Erro ao fazer a solicitação. Código de status: {response.status_code}")
            raise requests.HTTPError(f"Status code: {response.status_code}")

    except requests.RequestException as e:
        print(f"❌ Erro durante a solicitação: {e}")
        raise


@task
def transform_cemaden_data_task(dfr: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma e limpa os dados de precipitação do CEMADEN.

    Realiza as seguintes operações:
    - Filtra dados do município do Rio de Janeiro (código IBGE: 3304557)
    - Filtra apenas estações pluviométricas (tipo = 1)
    - Remove colunas desnecessárias (uf, codibge, cidade, etc.)
    - Renomeia colunas para padrão do projeto
    - Converte timezone de UTC para America/Sao_Paulo
    - Limpa valores inconsistentes ("-", NaN, negativos)
    - Remove duplicatas

    Args:
        dfr: DataFrame com dados brutos do CEMADEN

    Returns:
        DataFrame transformado e limpo

    Examples:
        >>> dfr_raw = download_cemaden_data_task()
        >>> dfr_clean = transform_cemaden_data_task(dfr_raw)
        >>> print(dfr_clean.columns)
        Index(['id_estacao', 'latitude', 'longitude', 'acumulado_chuva_10_min', ...])

    Notes:
        - Apenas estações do Rio de Janeiro (código IBGE: 3304557)
        - Apenas estações pluviométricas (tipoestacao == 1)
        - Timezone convertido de UTC para America/Sao_Paulo
        - Valores "-" e NaN substituídos por 0 (acumulados) ou None
        - Valores negativos de chuva removidos (tratados como None)
        - Duplicatas removidas por id_estacao + data_medicao
    """
    print("🔄 Iniciando transformação dos dados do CEMADEN...")

    # Filtrar apenas município do Rio de Janeiro (código IBGE: 3304557)
    print(f"   Total antes do filtro: {len(dfr)} registros")
    dfr = dfr[dfr["codibge"] == 3304557].copy()
    print(f"   Após filtro RJ: {len(dfr)} registros")

    # Filtrar apenas estações pluviométricas (tipo 1)
    dfr = dfr[dfr["tipoestacao"] == 1].copy()
    print(f"   Após filtro pluviométricas: {len(dfr)} registros")

    # Renomear colunas
    rename_cols = {
        "idestacao": "id_estacao",
        "latitude": "latitude",
        "longitude": "longitude",
        "ultimovalor": "acumulado_chuva_10_min",
        "acum15min": "acumulado_chuva_15_min",
        "acum1h": "acumulado_chuva_1h",
        "acum3h": "acumulado_chuva_3h",
        "acum6h": "acumulado_chuva_6h",
        "acum12h": "acumulado_chuva_12h",
        "acum24h": "acumulado_chuva_24h",
        "acum96h": "acumulado_chuva_96h",
        "datahora": "data_medicao",
    }

    dfr = dfr.rename(columns=rename_cols)

    # Remover colunas desnecessárias
    columns_to_drop = ["uf", "codibge", "cidade", "nomeestacao", "tipoestacao", "status"]
    dfr = dfr.drop(columns=[col for col in columns_to_drop if col in dfr.columns])

    # Converter data_medicao para datetime (UTC)
    dfr["data_medicao"] = pd.to_datetime(dfr["data_medicao"], format="%Y-%m-%d %H:%M:%S")

    # Converter timezone de UTC para America/Sao_Paulo
    dfr["data_medicao"] = (
        dfr["data_medicao"]
        .dt.tz_localize("UTC")
        .dt.tz_convert("America/Sao_Paulo")
        .dt.tz_localize(None)
    )

    # Listar colunas de acumulados de chuva
    acumulado_cols = [
        "acumulado_chuva_10_min",
        "acumulado_chuva_15_min",
        "acumulado_chuva_1h",
        "acumulado_chuva_3h",
        "acumulado_chuva_6h",
        "acumulado_chuva_12h",
        "acumulado_chuva_24h",
        "acumulado_chuva_96h",
    ]

    # Substituir valores "-" por 0 nas colunas de acumulados
    for col in acumulado_cols:
        if col in dfr.columns:
            dfr[col] = dfr[col].replace("-", 0)
            # Converter para numérico
            dfr[col] = pd.to_numeric(dfr[col], errors="coerce")
            # Substituir NaN por 0
            dfr[col] = dfr[col].fillna(0)
            # Remover valores negativos (tratar como None/0)
            dfr.loc[dfr[col] < 0, col] = 0

    # Converter latitude e longitude para numérico
    dfr["latitude"] = pd.to_numeric(dfr["latitude"], errors="coerce")
    dfr["longitude"] = pd.to_numeric(dfr["longitude"], errors="coerce")

    # Remover duplicatas por id_estacao e data_medicao
    before_dedup = len(dfr)
    dfr = dfr.drop_duplicates(subset=["id_estacao", "data_medicao"], keep="first")
    after_dedup = len(dfr)
    if before_dedup > after_dedup:
        print(f"   Removidas {before_dedup - after_dedup} duplicatas")

    # Converter data_medicao para string
    dfr["data_medicao"] = dfr["data_medicao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Ordenar colunas
    keep_cols = list(rename_cols.values())
    dfr = dfr[[col for col in keep_cols if col in dfr.columns]]

    print(f"✅ Transformação concluída: {len(dfr)} registros")
    print(f"   Estações únicas: {dfr['id_estacao'].nunique()}")

    if not dfr.empty:
        print(f"   Data mínima: {dfr['data_medicao'].min()}")
        print(f"   Data máxima: {dfr['data_medicao'].max()}")

    return dfr


@task
def save_data_to_partitions_task(
    dfr: pd.DataFrame,
    partition_column: str = "data_medicao",
) -> Path:
    """
    Salva os dados em partições CSV organizadas por data.

    Os dados são particionados pela coluna especificada e salvos em uma estrutura
    de diretórios que facilita o carregamento no BigQuery com particionamento.

    Args:
        dfr: DataFrame com dados processados
        partition_column: Nome da coluna para particionamento

    Returns:
        Path: Caminho do diretório raiz onde as partições foram salvas

    Examples:
        >>> dfr_clean = transform_cemaden_data_task(dfr_raw)
        >>> prepath = save_data_to_partitions_task(dfr_clean)
        >>> print(prepath)
        /tmp/precipitacao_cemaden

    Notes:
        - Formato de partição: ano=YYYY/mes=MM/data=YYYY-MM-DD/dados.csv
        - Os arquivos são salvos temporariamente em /tmp/precipitacao_cemaden/
        - Timestamp adicionado ao nome do arquivo para evitar sobrescrita
    """
    prepath = Path("/tmp/precipitacao_cemaden")
    prepath.mkdir(parents=True, exist_ok=True)

    print(f"💾 Salvando dados em partições...")
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
    de estações do Rio de Janeiro. Se detectar novas estações, emite um aviso.

    Args:
        dfr: DataFrame com dados processados contendo coluna 'id_estacao'

    Returns:
        None

    Examples:
        >>> dfr_clean = transform_cemaden_data_task(dfr_raw)
        >>> check_for_new_stations_task(dfr_clean)
        ✅ Nenhuma nova estação detectada

    Notes:
        - Lista de estações conhecidas: 23 estações no Rio de Janeiro
        - Se novas estações forem detectadas, apenas loga aviso
        - Não interrompe o flow (comportamento diferente do Prefect 1.4)
        - Verificação importante para manutenção do cadastro de estações
    """
    # Lista de estações conhecidas do Rio de Janeiro (CEMADEN)
    known_stations = [
        3043, 3044, 3045, 3114, 3215,
        7593, 7594, 7595, 7596, 7597,
        7598, 7599, 7600, 7601, 7602,
        7603, 7606, 7609, 7610, 7611,
        7612, 7613, 7614, 7615,
    ]

    print("🔍 Verificando novas estações...")
    print(f"   Estações conhecidas: {len(known_stations)}")

    # Obter IDs únicos das estações nos dados
    current_stations = dfr["id_estacao"].unique().tolist()
    print(f"   Estações nos dados: {len(current_stations)}")

    # Verificar se há estações novas
    new_stations = [station for station in current_stations if station not in known_stations]

    if len(new_stations) == 0:
        print("✅ Nenhuma nova estação detectada")
    else:
        print(f"⚠️  Nova(s) estação(ões) identificada(s): {new_stations}")
        print("   Você precisa atualizar o cadastro de estações CEMADEN!")
        print("   Atualize a lista de estações conhecidas em:")
        print("   - tasks.py: função check_for_new_stations_task()")
        print("   - Tabela estacoes_cemaden no BigQuery (se existir)")
