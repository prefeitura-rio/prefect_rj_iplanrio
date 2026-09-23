"""Cliente e helpers de Google Drive.

Cópia adaptada de ``rj_crm__relatorio_engajamento_hsm/utils/drive.py`` — mesmo padrão
(mesma API, mesmas funções), sem alterar o original (regra do projeto: pipelines
existentes não são tocadas — ver TODO). A pasta raiz (ID vem do flow como parâmetro)
precisa já existir e estar compartilhada (Editor) com a service account de
``BASEDOSDADOS_CREDENTIALS_<PROD|STAGING>``; subpastas (aqui, uma por versão do modelo)
são criadas automaticamente dentro dela.
"""

import io

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"


def get_drive_service(environment: str) -> Resource:
    """Autentica no Drive com a credencial do secret do work pool.

    :param environment: ``"prod"`` ou ``"staging"``.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    return build("drive", "v3", credentials=credentials)


def busca_pasta(drive: Resource, nome: str, parent_id: str | None) -> str | None:
    """Procura uma pasta pelo nome (e, se dado, pelo pai).

    :returns: O ID da pasta, ou ``None`` se não existir.
    """
    query = f"name = '{nome}' and mimeType = '{DRIVE_FOLDER_MIME}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    resultado = drive.files().list(q=query, fields="files(id, name)", spaces="drive").execute()
    arquivos = resultado.get("files", [])
    return arquivos[0]["id"] if arquivos else None


def confirma_pasta_raiz(drive: Resource, pasta_raiz_id: str) -> None:
    """Confere que a pasta raiz (ID passado pelo flow — não busca por nome, pra não correr
    o risco de pegar outra pasta com o mesmo nome compartilhada com a service account por
    engano) existe e está acessível, antes de tentar criar subpasta/publicar arquivo nela.

    :raises RuntimeError: Se a pasta não existir ou não estiver acessível.
    """
    try:
        drive.files().get(fileId=pasta_raiz_id, fields="id").execute()
    except HttpError as exc:
        raise RuntimeError(
            f"Pasta de ID '{pasta_raiz_id}' inacessível — confira se ela ainda existe e se está "
            "compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_<PROD|STAGING>."
        ) from exc


def pasta_por_nome(drive: Resource, raiz_id: str, nome: str) -> str:
    """Acha (ou cria, se não existir) uma subpasta pelo nome, direto na pasta raiz.

    :returns: O ID da subpasta.
    """
    pasta_id = busca_pasta(drive, nome, parent_id=raiz_id)
    if pasta_id:
        return pasta_id
    metadata = {"name": nome, "mimeType": DRIVE_FOLDER_MIME, "parents": [raiz_id]}
    pasta = drive.files().create(body=metadata, fields="id").execute()
    logger.info("Subpasta '%s' criada.", nome)
    return pasta["id"]


def upload_bytes(drive: Resource, pasta_id: str, nome_arquivo: str, conteudo: bytes, mime_type: str) -> None:
    """Sobe um arquivo (bytes em memória, sem passar por disco) pra uma pasta do Drive."""
    media = MediaIoBaseUpload(io.BytesIO(conteudo), mimetype=mime_type, resumable=False)
    metadata = {"name": nome_arquivo, "parents": [pasta_id]}
    drive.files().create(body=metadata, media_body=media, fields="id").execute()
    logger.info("'%s' publicado no Drive.", nome_arquivo)
