"""Armazenamento das versões do modelo no GCS (ou num diretório local, para testes).

Layout sob a raiz (``gs://bucket/prefixo`` ou um caminho local)::

    champion.json             ponteiro: {"versao": "2026-10-01", "promovido_em": "..."}
    champion_anterior.json    o ponteiro anterior, para rollback
    <versao>/modelo.txt       LightGBM em formato nativo (sobrevive a upgrade de biblioteca)
    <versao>/metadata.json    features na ordem do modelo, hiperparâmetros, métricas etc.

Uma versão publicada nunca é sobrescrita. Promover é trocar o ponteiro, e o ponteiro é
sempre a ÚLTIMA coisa escrita, para uma falha no meio nunca apontar para versão incompleta.
Rollback é ``promove_versao`` com a versão anterior.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import lightgbm as lgb
from google.cloud import storage
from google.cloud.exceptions import NotFound

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

ARQUIVO_MODELO = "modelo.txt"
ARQUIVO_METADATA = "metadata.json"
ARQUIVO_CHAMPION = "champion.json"
ARQUIVO_CHAMPION_ANTERIOR = "champion_anterior.json"
PREFIXO_GCS = "gs://"
FUSO = ZoneInfo("America/Sao_Paulo")


@dataclass(frozen=True)
class ModeloCarregado:
    """Uma versão do modelo pronta para prever."""

    versao: str
    booster: lgb.Booster
    metadata: dict


def divide_gcs(raiz: str) -> tuple[str, str]:
    """Separa ``gs://bucket/prefixo`` em ``(bucket, prefixo)``.

    :param raiz: URI começando com ``gs://``.
    :returns: Bucket e prefixo (sem barras nas pontas; vazio se não houver).
    """
    bucket, _, prefixo = raiz[len(PREFIXO_GCS) :].partition("/")
    return bucket, prefixo.strip("/")


def caminho_objeto(raiz: str, *partes: str) -> str:
    """Monta o caminho de um arquivo sob a raiz (chave no GCS ou caminho local)."""
    if raiz.startswith(PREFIXO_GCS):
        _, prefixo = divide_gcs(raiz)
        return "/".join(p for p in (prefixo, *partes) if p)
    return str(Path(raiz).joinpath(*partes))


def le_texto(raiz: str, *partes: str, client: storage.Client | None = None) -> str | None:
    """Lê um arquivo de texto sob a raiz.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param partes: Caminho do arquivo relativo à raiz.
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :returns: O conteúdo, ou ``None`` se o arquivo não existir.
    :raises ValueError: Se a raiz for ``gs://`` e ``client`` não for passado.
    """
    if raiz.startswith(PREFIXO_GCS):
        blob = _blob(raiz, partes, client)
        try:
            return blob.download_as_text()
        except NotFound:
            return None
    arquivo = Path(caminho_objeto(raiz, *partes))
    return arquivo.read_text(encoding="utf-8") if arquivo.exists() else None


def grava_texto(raiz: str, *partes: str, conteudo: str, client: storage.Client | None = None) -> None:
    """Grava (ou sobrescreve) um arquivo de texto sob a raiz.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param partes: Caminho do arquivo relativo à raiz.
    :param conteudo: Texto a gravar.
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :raises ValueError: Se a raiz for ``gs://`` e ``client`` não for passado.
    """
    if raiz.startswith(PREFIXO_GCS):
        _blob(raiz, partes, client).upload_from_string(conteudo, content_type="text/plain; charset=utf-8")
        return
    arquivo = Path(caminho_objeto(raiz, *partes))
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    arquivo.write_text(conteudo, encoding="utf-8")


def publica_versao(
    raiz: str, versao: str, booster: lgb.Booster, metadata: dict, client: storage.Client | None = None
) -> None:
    """Grava ``modelo.txt`` e ``metadata.json`` de uma versão nova, sem promovê-la.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param versao: Nome da pasta da versão, por exemplo ``2026-10-01``.
    :param booster: Modelo treinado.
    :param metadata: Ficha da versão; precisa ter ``features`` (ordem que o modelo espera).
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :raises FileExistsError: Se a versão já foi publicada (versões são imutáveis).
    :raises ValueError: Se ``versao`` for inválida, ``metadata`` não tiver ``features`` ou
        as features do modelo não baterem com as do ``metadata``.
    """
    if not versao or "/" in versao:
        raise ValueError(f"versão inválida: {versao!r}")
    if "features" not in metadata:
        raise ValueError("metadata precisa ter a chave 'features'")
    if booster.feature_name() != metadata["features"]:
        raise ValueError("as features do modelo não batem com metadata['features']")
    if le_texto(raiz, versao, ARQUIVO_MODELO, client=client) is not None:
        raise FileExistsError(f"a versão {versao} já existe em {raiz}")

    grava_texto(raiz, versao, ARQUIVO_MODELO, conteudo=booster.model_to_string(), client=client)
    grava_texto(
        raiz,
        versao,
        ARQUIVO_METADATA,
        conteudo=json.dumps(metadata, indent=2, ensure_ascii=False, default=str),
        client=client,
    )
    logger.info("Versão %s publicada em %s", versao, raiz)


def carrega_versao(raiz: str, versao: str, client: storage.Client | None = None) -> ModeloCarregado:
    """Carrega uma versão específica do modelo.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param versao: Nome da pasta da versão.
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :returns: O modelo, a versão e o ``metadata``.
    :raises FileNotFoundError: Se a versão não existir.
    :raises ValueError: Se as features do modelo não baterem com as do ``metadata``.
    """
    modelo_txt = le_texto(raiz, versao, ARQUIVO_MODELO, client=client)
    metadata_txt = le_texto(raiz, versao, ARQUIVO_METADATA, client=client)
    if modelo_txt is None or metadata_txt is None:
        raise FileNotFoundError(f"versão {versao} incompleta ou inexistente em {raiz}")

    booster = lgb.Booster(model_str=modelo_txt)
    metadata = json.loads(metadata_txt)
    if booster.feature_name() != metadata["features"]:
        raise ValueError(f"versão {versao}: features do modelo não batem com o metadata")
    return ModeloCarregado(versao=versao, booster=booster, metadata=metadata)


def carrega_champion(raiz: str, client: storage.Client | None = None) -> ModeloCarregado | None:
    """Carrega o modelo apontado por ``champion.json``.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :returns: O champion, ou ``None`` se ainda não houver ponteiro.
    :raises FileNotFoundError: Se o ponteiro apontar para uma versão que não existe.
    """
    ponteiro = le_texto(raiz, ARQUIVO_CHAMPION, client=client)
    if ponteiro is None:
        return None
    return carrega_versao(raiz, json.loads(ponteiro)["versao"], client=client)


def promove_versao(raiz: str, versao: str, client: storage.Client | None = None) -> None:
    """Aponta ``champion.json`` para a versão, guardando o ponteiro anterior.

    Serve também de rollback: promover de novo a versão anterior.

    :param raiz: ``gs://bucket/prefixo`` ou diretório local.
    :param versao: Versão já publicada.
    :param client: Cliente do GCS, obrigatório se a raiz for ``gs://``.
    :raises FileNotFoundError: Se a versão não estiver publicada por completo.
    """
    carrega_versao(raiz, versao, client=client)  # garante que a versão existe e está íntegra

    ponteiro_atual = le_texto(raiz, ARQUIVO_CHAMPION, client=client)
    if ponteiro_atual is not None:
        grava_texto(raiz, ARQUIVO_CHAMPION_ANTERIOR, conteudo=ponteiro_atual, client=client)

    novo = {"versao": versao, "promovido_em": datetime.now(FUSO).isoformat(timespec="seconds")}
    grava_texto(raiz, ARQUIVO_CHAMPION, conteudo=json.dumps(novo, indent=2), client=client)
    logger.info("Champion agora é a versão %s", versao)


def _blob(raiz: str, partes: tuple[str, ...], client: storage.Client | None) -> storage.Blob:
    if client is None:
        raise ValueError("client do GCS é obrigatório para raízes gs://")
    bucket, _ = divide_gcs(raiz)
    return client.bucket(bucket).blob(caminho_objeto(raiz, *partes))
