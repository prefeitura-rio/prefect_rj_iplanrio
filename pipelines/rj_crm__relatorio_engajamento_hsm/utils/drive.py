# -*- coding: utf-8 -*-
"""Cliente e helpers de Google Drive — a pasta raiz (ID vem do flow como parâmetro,
default config.DRIVE_PASTA_RAIZ_ID — ver flow.py) precisa já existir e estar
compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_PROD;
subpastas por HSM são criadas automaticamente dentro dela.

A pasta raiz vive numa Unidade Compartilhada (não "Meu Drive") — por isso toda chamada
files().get/list/create abaixo passa supportsAllDrives=True (e includeItemsFromAllDrives
no list). Sem esse parâmetro a API do Drive trata item de Unidade Compartilhada como
inexistente e devolve 404 mesmo com a service account tendo acesso — não confundir com
falta de compartilhamento de fato."""

from __future__ import annotations

import io

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_crm__relatorio_engajamento_hsm.config import DRIVE_FOLDER_MIME


def get_drive_service() -> Resource:
    credentials = get_bd_credentials_from_env(mode="prod")
    return build("drive", "v3", credentials=credentials)


def busca_pasta(drive: Resource, nome: str, parent_id: str | None) -> str | None:
    query = f"name = '{nome}' and mimeType = '{DRIVE_FOLDER_MIME}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    resultado = drive.files().list(
        q=query,
        fields="files(id, name)",
        spaces="drive",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    arquivos = resultado.get("files", [])
    return arquivos[0]["id"] if arquivos else None


def confirma_pasta_raiz(drive: Resource, pasta_raiz_id: str) -> None:
    """Confere que a pasta raiz (ID passado pelo flow — não busca por nome, pra não
    correr o risco de pegar outra pasta com o mesmo nome compartilhada por engano com a
    service account) existe e está acessível, antes de tentar criar subpasta/publicar
    arquivo nela."""
    try:
        drive.files().get(fileId=pasta_raiz_id, fields="id", supportsAllDrives=True).execute()
    except HttpError as exc:
        raise RuntimeError(
            f"Pasta de ID '{pasta_raiz_id}' inacessível — confira se ela ainda existe e se está "
            "compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_PROD."
        ) from exc


def pasta_hsm_id(drive: Resource, raiz_id: str, nome_hsm: str) -> str:
    pasta_id = busca_pasta(drive, nome_hsm, parent_id=raiz_id)
    if pasta_id:
        return pasta_id
    metadata = {"name": nome_hsm, "mimeType": DRIVE_FOLDER_MIME, "parents": [raiz_id]}
    pasta = drive.files().create(body=metadata, fields="id", supportsAllDrives=True).execute()
    log(f"[DRIVE] subpasta '{nome_hsm}' criada.")
    return pasta["id"]


def upload_bytes(drive: Resource, pasta_id: str, nome_arquivo: str, conteudo: bytes, mime_type: str) -> None:
    media = MediaIoBaseUpload(io.BytesIO(conteudo), mimetype=mime_type, resumable=False)
    metadata = {"name": nome_arquivo, "parents": [pasta_id]}
    drive.files().create(body=metadata, media_body=media, fields="id", supportsAllDrives=True).execute()
    log(f"[DRIVE] '{nome_arquivo}' publicado.")
