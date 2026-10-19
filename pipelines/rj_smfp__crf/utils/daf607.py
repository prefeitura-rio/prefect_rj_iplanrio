"""Utilitários do domínio DAF607 — arquivos .ret posicionais em latin-1."""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
from google.cloud.storage import Bucket
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import to_partitions

from pipelines.rj_smfp__crf.utils.shared import (
    EXTRACT_BASE_PATH,
    cleanup_extracted_directory,
    get_gcs_bucket,
)

# ---------------------------------------------------------------------------
# Constantes internas
# ---------------------------------------------------------------------------

DAF607_TABLES = {"daf607_header", "daf607_detalhe", "daf607_trailer"}

_DECIMAL_TYPE = pa.decimal128(17, 2)

_DETALHE_VALOR_COLS = [
    "valor_principal",
    "valor_multa",
    "valor_juros",
    "valor_autenticacao",
]

_TRAILER_VALOR_COLS = ["valor_total_recebido"]


# ---------------------------------------------------------------------------
# Dataclass de configuração de execução
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Daf607Config:
    """Configuração de execução do pipeline DAF607.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Nome do bucket GCS contendo os arquivos ``.ret``.
    :param folder_prefix: Prefixo da pasta no bucket (ex.: ``'DAF607'``).
    :param data_inicio: Data de início do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com ``data_geracao`` igual ou anterior são ignorados (exclusivo).
    :param data_fim: Data de fim do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com ``data_geracao`` posterior são ignorados (inclusivo).
    :param extract_base_path: Diretório base local para download temporário e
        saída parquet.
    """

    project_id: str
    bucket_name: str
    folder_prefix: str
    data_inicio: date | str | None = None
    data_fim: date | str | None = None
    extract_base_path: str = EXTRACT_BASE_PATH


# ---------------------------------------------------------------------------
# Funções auxiliares internas (convenção de módulo, sem prefixo _)
# ---------------------------------------------------------------------------


def centavos_str_to_decimal128(series: pd.Series) -> pd.arrays.ArrowExtensionArray:
    """Converte strings de centavos inteiros para ``decimal128(17, 2)``.

    Insere o ponto decimal duas casas antes do final da string e passa
    diretamente para ``pa.array`` com o tipo alvo — sem divisão aritmética.
    Exemplo: ``'250000'`` → ``'2500.00'`` → ``Decimal('2500.00')``.

    :param series: Série pandas com strings de centavos inteiros.
    :returns: Array pandas backed por ``decimal128(17, 2)`` via PyArrow.
    """

    def fmt(v: str) -> str:
        v = v.strip() or "0"
        v = v.lstrip("0") or "0"
        if len(v) <= 2:
            return f"0.{v.zfill(2)}"
        return f"{v[:-2]}.{v[-2:]}"

    return pd.array(
        pa.array([fmt(v) for v in series], type=pa.string()).cast(_DECIMAL_TYPE).to_pylist(),
        dtype=pd.ArrowDtype(_DECIMAL_TYPE),
    )


def add_partition_columns_daf607(df: pd.DataFrame, data_geracao: str | None) -> pd.DataFrame:
    """Adiciona colunas de partição a um DataFrame DAF607.

    Usa ``data_geracao`` (formato ``DDMMAAAA``) como base para as colunas
    ``ano_particao``, ``mes_particao`` e ``data_particao``. Se o valor for
    inválido, as colunas são preenchidas com ``None``.

    :param df: DataFrame de entrada.
    :param data_geracao: String no formato ``DDMMAAAA`` extraída do header.
    :returns: DataFrame com as colunas de partição adicionadas.
    """
    try:
        if data_geracao and len(data_geracao) == 8:
            year = data_geracao[0:4]
            month = data_geracao[4:6]
            day = data_geracao[6:8]
            df["ano_particao"] = year
            df["mes_particao"] = month
            df["data_particao"] = f"{year}-{month}-{day}"
        else:
            df["ano_particao"] = None
            df["mes_particao"] = None
            df["data_particao"] = None
    except Exception:
        df["ano_particao"] = None
        df["mes_particao"] = None
        df["data_particao"] = None
    return df


def read_data_geracao_from_daf607_blob(blob: Bucket) -> date | None:
    """Extrai ``data_geracao`` do header de um blob DAF607 via range request.

    Baixa apenas os primeiros 200 bytes do blob (suficiente para cobrir a
    primeira linha de header), decodifica em ``latin-1`` e lê linha a linha
    até encontrar o registro do tipo ``'1'``. Extrai ``data_geracao`` nas
    posições 29-37 (formato ``DDMMYYYY``) e converte para ``datetime.date``.

    O range request evita carregar o arquivo inteiro em memória — o custo é
    fixo e independente do tamanho do ``.ret``.

    :param blob: Objeto ``google.cloud.storage.Blob`` do arquivo ``.ret``.
    :returns: Data de geração como ``datetime.date``, ou ``None`` se o campo
        não for encontrado ou contiver um valor inválido.
    """
    raw = blob.download_as_bytes(start=0, end=199)
    for linha in raw.decode("latin-1").splitlines():
        if not linha.strip():
            continue
        if linha[0:1] == "1":
            date_str = linha[29:37].strip()
            if len(date_str) == 8:
                try:
                    return date(
                        int(date_str[0:4]),
                        int(date_str[4:6]),
                        int(date_str[6:8]),
                    )
                except (ValueError, TypeError):
                    log(f"DAF607: data_geracao inválida em '{blob.name}': {date_str!r}", level="warning")
            return None
    log(f"DAF607: header ausente em '{blob.name}'", level="warning")
    return None


def bisect_daf607_blobs(
    blobs: list,
    target: date,
    find_first_greater: bool,
) -> int:
    """Busca binária sobre lista de blobs DAF607 ordenada por nome (sequencial crescente).

    Abre apenas ``O(log n)`` blobs via range request de 200 bytes para
    determinar o índice de corte. Cada chamada lê ``data_geracao`` do blob
    do meio e descarta metade da lista, sem nunca varrer todos os arquivos.

    :param blobs: Lista de objetos ``Blob`` ordenada por nome ascendente.
    :param target: Data de referência para comparação.
    :param find_first_greater: Se ``True``, retorna o índice do primeiro blob
        com ``data_geracao > target`` (usado para ``data_inicio`` exclusivo).
        Se ``False``, retorna o índice logo após o último blob com
        ``data_geracao <= target`` (usado para o limite superior inclusivo).
    :returns: Índice de corte na lista ``blobs``.
    """
    lo, hi = 0, len(blobs)
    while lo < hi:
        mid = (lo + hi) // 2
        mid_date = read_data_geracao_from_daf607_blob(blobs[mid])
        if mid_date is None:
            lo = mid + 1
            continue
        log(f"DAF607: bisect [{mid}] {mid_date} vs {target}", level="debug")
        if mid_date <= target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def list_daf607_ret_files_in_gcs(
    bucket: Bucket,
    folder_prefix: str,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
) -> list[str]:
    """Lista arquivos ``.ret`` DAF607 no GCS filtrando por intervalo de datas.

    Lista todos os nomes de blob via metadados do GCS (sem abrir arquivos) e,
    quando há filtro de data, aplica **busca binária** sobre a lista ordenada
    pelo nome sequencial — que cresce cronologicamente. Abre no máximo
    ``2 * log₂(n)`` blobs via range request de 200 bytes para determinar os
    índices de corte inferior e superior, independentemente do volume total.

    Exemplo: com 19594 arquivos, abre no máximo ~30 blobs em vez de 19594.

    :param bucket: Google Cloud Storage bucket object.
    :param folder_prefix: Prefixo da pasta no bucket (ex.: ``'DAF607'``).
    :param data_inicio: Início do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com ``data_geracao <= data_inicio`` são ignorados (exclusivo).
    :param data_fim: Fim do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com ``data_geracao > data_fim`` são ignorados (inclusivo).
    :returns: Lista de nomes de blob dentro do intervalo.
    """

    def parse_date(value: date | str | None, param_name: str) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            raise ValueError(
                f"Formato de data inválido para '{param_name}': '{value}'. Use o formato YYYY-MM-DD."
            )

    data_inicio = parse_date(data_inicio, "data_inicio")
    data_fim = parse_date(data_fim, "data_fim")

    all_blobs = sorted(
        (
            blob
            for blob in bucket.list_blobs(prefix=folder_prefix)
            if blob.name.split("/")[-1].upper().startswith("DAF607")
            and blob.name.lower().endswith(".ret")
        ),
        key=lambda b: b.name,
    )

    total = len(all_blobs)

    if not all_blobs or (data_inicio is None and data_fim is None):
        log(f"DAF607: {total} arquivo(s) .ret encontrados em '{folder_prefix}' (sem filtro de data)")
        return [blob.name for blob in all_blobs]

    idx_start = 0
    if data_inicio is not None:
        idx_start = bisect_daf607_blobs(all_blobs, data_inicio, find_first_greater=True)

    idx_end = total
    if data_fim is not None:
        idx_end = bisect_daf607_blobs(all_blobs, data_fim, find_first_greater=False)

    filtered = [blob.name for blob in all_blobs[idx_start:idx_end]]
    log(f"DAF607: {len(filtered)} de {total} arquivo(s) .ret no intervalo ({data_inicio}, {data_fim}]")
    return filtered


def parse_daf607_file(
    file_path: str,
    blob_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Faz o parsing de um arquivo DAF607 de largura fixa com encoding latin-1.

    Lê o arquivo linha a linha e separa os registros em três DataFrames:
    - **header** (tipo ``'1'``): dados do cabeçalho do lote.
    - **detalhe** (tipo ``'2'``): registros individuais de arrecadação.
    - **trailer** (tipo ``'9'``): totalizadores do lote.

    :param file_path: Caminho local do arquivo DAF607.
    :param blob_name: Nome completo do blob no GCS, gravado na coluna ``arquivo_origem``.
    :returns: Tupla ``(df_header, df_detalhe, df_trailer)`` com os três DataFrames.
    """
    headers = []
    detalhes = []
    trailers = []

    loaded_at = datetime.now().isoformat()

    with open(file_path, "r", encoding="latin-1") as arquivo:
        for linha in arquivo:
            if not linha.strip():
                continue

            tipo_registro = linha[0:1]

            if tipo_registro == "1":
                headers.append(
                    {
                        "codigo_registro": linha[0:1],
                        "sequencial": linha[1:9],
                        "codigo_convenio": linha[9:29].strip(),
                        "data_geracao": linha[29:37],
                        "numero_remessa": linha[37:43],
                        "versao": linha[43:45],
                        "banco_arrecadador": linha[75:78],
                        "data_credito": linha[78:86],
                        "arquivo_origem": blob_name,
                        "loaded_at": loaded_at,
                    }
                )

            elif tipo_registro == "2":
                detalhes.append(
                    {
                        "codigo_registro": linha[0:1],
                        "sequencial": linha[1:9],
                        "data_arrecadacao": linha[9:17],
                        "data_vencimento": linha[17:25],
                        "cnpj_contribuinte": linha[74:88],
                        "esfera": linha[99:100],
                        "competencia": linha[100:106],
                        "valor_principal": linha[106:123].strip() or "0",
                        "valor_multa": linha[123:140].strip() or "0",
                        "valor_juros": linha[140:157].strip() or "0",
                        "valor_autenticacao": linha[204:221].strip() or "0",
                        "banco_arrecadador": linha[244:247],
                        "agencia_arrecadadora": linha[247:251],
                        "codigo_siafi": linha[455:461],
                        "identificador_serpro": linha[461:478],
                        "arquivo_origem": blob_name,
                        "loaded_at": loaded_at,
                    }
                )

            elif tipo_registro == "9":
                trailers.append(
                    {
                        "codigo_registro": linha[0:1],
                        "sequencial": linha[1:9],
                        "total_registros": int(linha[9:15] or 0),
                        "valor_total_recebido": linha[15:32].strip() or "0",
                        "arquivo_origem": blob_name,
                        "loaded_at": loaded_at,
                    }
                )

    df_header = pd.DataFrame(headers)
    df_detalhe = pd.DataFrame(detalhes)
    df_trailer = pd.DataFrame(trailers)

    if not df_detalhe.empty:
        for col in _DETALHE_VALOR_COLS:
            df_detalhe[col] = centavos_str_to_decimal128(df_detalhe[col])

    if not df_trailer.empty:
        for col in _TRAILER_VALOR_COLS:
            df_trailer[col] = centavos_str_to_decimal128(df_trailer[col])

    return df_header, df_detalhe, df_trailer


def read_data_geracao_from_daf607(file_path: str) -> str | None:
    """Lê apenas o campo ``data_geracao`` do primeiro registro header de um arquivo DAF607.

    Abre o arquivo linha a linha e retorna imediatamente ao encontrar o
    primeiro registro do tipo ``'1'`` (header), extraindo os bytes 29-37.
    O restante do arquivo nunca é lido, mantendo consumo de memória mínimo
    independentemente do tamanho do arquivo.

    :param file_path: Caminho local do arquivo DAF607.
    :returns: String de 8 caracteres no formato ``DDMMAAAA``, ou ``None`` se
        nenhum registro header for encontrado ou o campo estiver em branco.
    """
    with open(file_path, "r", encoding="latin-1") as f:
        for linha in f:
            if not linha.strip():
                continue
            if linha[0:1] == "1":
                value = linha[29:37].strip()
                return value if value else None
    return None


def process_daf607_files_from_gcs(config: Daf607Config) -> dict[str, str | None]:
    """Processa arquivos DAF607 ``.ret`` do GCS e retorna os caminhos parquet por subtabela.

    Os arquivos ``.ret`` ficam diretamente na pasta ``folder_prefix`` do bucket,
    sem compactação. O nome segue o padrão ``DAF607<seq><DDMMYYYY><HHMMSS>.ret``.

    Para cada arquivo dentro do intervalo de datas:

    1. Lista os ``.ret`` no GCS com busca binária por data (``O(log n)`` range requests).
    2. Faz download do ``.ret`` para path temporário local.
    3. Lê apenas o campo ``data_geracao`` da primeira linha tipo ``'1'``.
    4. Faz o parsing completo via :func:`parse_daf607_file`, separando em header,
       detalhe e trailer.
    5. Adiciona colunas de partição derivadas de ``data_geracao``.
    6. Salva os três conjuntos como parquets particionados via ``to_partitions``.
    7. Remove o arquivo temporário em ``try/finally``.

    :param config: Instância de :class:`Daf607Config` com todos os parâmetros de execução.
    :returns: Dicionário ``{"daf607_header": path, "daf607_detalhe": path,
        "daf607_trailer": path}`` com os caminhos dos parquets gerados, ou
        ``None`` por subtabela caso nenhum dado tenha sido encontrado.
    """
    bucket = get_gcs_bucket(config.project_id, config.bucket_name)

    ret_blobs = list_daf607_ret_files_in_gcs(
        bucket=bucket,
        folder_prefix=config.folder_prefix,
        data_inicio=config.data_inicio,
        data_fim=config.data_fim,
    )

    if not ret_blobs:
        log("DAF607: nenhum arquivo .ret no intervalo especificado", level="warning")
        return {"daf607_header": None, "daf607_detalhe": None, "daf607_trailer": None}

    log(f"DAF607: processando {len(ret_blobs)} arquivo(s) .ret")

    data_paths: dict[str, str | None] = {
        "daf607_header": None,
        "daf607_detalhe": None,
        "daf607_trailer": None,
    }
    totais: dict[str, int] = {"daf607_header": 0, "daf607_detalhe": 0, "daf607_trailer": 0}

    tmp_dir = Path(config.extract_base_path) / "daf607_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    try:
        for blob_name in ret_blobs:
            ret_filename = blob_name.split("/")[-1]
            local_path = tmp_dir / ret_filename

            try:
                bucket.blob(blob_name).download_to_filename(str(local_path))
                data_geracao: str | None = read_data_geracao_from_daf607(str(local_path))
                df_header, df_detalhe, df_trailer = parse_daf607_file(str(local_path), blob_name)

                for subtable, df in [
                    ("daf607_header", df_header),
                    ("daf607_detalhe", df_detalhe),
                    ("daf607_trailer", df_trailer),
                ]:
                    if df.empty:
                        log(f"DAF607: {subtable} vazio em '{ret_filename}'", level="warning")
                        continue

                    df = add_partition_columns_daf607(df, data_geracao)
                    path = f"{config.extract_base_path}/{subtable}"
                    to_partitions(
                        data=df,
                        savepath=path,
                        data_type="parquet",
                        partition_columns=["ano_particao", "mes_particao", "data_particao"],
                    )
                    data_paths[subtable] = path
                    totais[subtable] += len(df)

            finally:
                local_path.unlink(missing_ok=True)

    finally:
        cleanup_extracted_directory(str(tmp_dir))

    resumo = ", ".join(f"{k.split('_')[-1]}: {v}" for k, v in totais.items())
    log(f"DAF607: concluído — {resumo} linhas")
    return data_paths
