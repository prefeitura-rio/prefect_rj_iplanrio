# -*- coding: utf-8 -*-
"""Cliente e helpers de Google Drive — a pasta raiz (ID vem do flow como parâmetro,
default config.DRIVE_PASTA_RAIZ_ID — ver flow.py) precisa já existir e estar
compartilhada (Editor) com a service account de BASEDOSDADOS_CREDENTIALS_PROD;
subpastas por campanha (e, dentro delas, por rodada de relatório) são criadas
automaticamente.

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

from pipelines.rj_crm__relatorio_engajamento_hsm.config import DRIVE_FOLDER_MIME

_CAMPOS_ARQUIVO = "id, name, mimeType, webViewLink, modifiedTime"


def get_drive_service() -> Resource:
    credentials = get_bd_credentials_from_env(mode="prod")
    return build("drive", "v3", credentials=credentials)


def _escapa(valor: str) -> str:
    """Escapa um valor pra dentro de aspas simples numa query `q` do Drive — nome de
    campanha/arquivo com apóstrofo quebraria a query (ou mudaria o que ela busca)."""
    return valor.replace("\\", "\\\\").replace("'", "\\'")


def _lista(drive: Resource, query: str) -> list[dict]:
    """files().list paginado — sem seguir nextPageToken, pasta com muitos itens
    devolveria só a 1ª página (100 por padrão) sem aviso nenhum."""
    arquivos: list[dict] = []
    page_token = None
    while True:
        resultado = drive.files().list(
            q=query,
            fields=f"nextPageToken, files({_CAMPOS_ARQUIVO})",
            spaces="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        arquivos.extend(resultado.get("files", []))
        page_token = resultado.get("nextPageToken")
        if not page_token:
            return arquivos


def busca_arquivo(drive: Resource, nome: str, parent_id: str, mime_type: str | None = None) -> dict | None:
    """Arquivo com nome EXATO dentro de parent_id (só filhos diretos) — ou None.
    Nome exato de propósito: é o que faz 'Catálogo_old' não contar como 'Catálogo'."""
    query = f"name = '{_escapa(nome)}' and '{parent_id}' in parents and trashed = false"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    arquivos = _lista(drive, query)
    if len(arquivos) > 1:
        print(f"[DRIVE] {len(arquivos)} arquivos chamados '{nome}' na mesma pasta — usando o 1º ({arquivos[0]['id']}).")
    return arquivos[0] if arquivos else None


def busca_pasta(drive: Resource, nome: str, parent_id: str | None) -> str | None:
    query = f"name = '{_escapa(nome)}' and mimeType = '{DRIVE_FOLDER_MIME}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    arquivos = _lista(drive, query)
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


def garante_pasta(drive: Resource, parent_id: str, nome: str) -> str:
    """ID da subpasta `nome` dentro de parent_id, criando se ainda não existir."""
    pasta_id = busca_pasta(drive, nome, parent_id=parent_id)
    if pasta_id:
        return pasta_id
    metadata = {"name": nome, "mimeType": DRIVE_FOLDER_MIME, "parents": [parent_id]}
    pasta = drive.files().create(body=metadata, fields="id", supportsAllDrives=True).execute()
    print(f"[DRIVE] subpasta '{nome}' criada.")
    return pasta["id"]


def lista_recursivo(drive: Resource, raiz_id: str, nome_contem: str, mime_type: str | None = None) -> list[dict]:
    """Todos os arquivos com `nome_contem` no nome em QUALQUER nível abaixo de raiz_id.
    A API do Drive não tem busca recursiva por pasta — percorre as subpastas em largura.
    Cada arquivo volta com `pasta_id` (pasta onde foi encontrado) e `caminho` (nomes
    das pastas entre raiz_id, exclusive, e o arquivo — ex.: ['<data>__disparos_...'])."""
    filtro = f"name contains '{_escapa(nome_contem)}' and trashed = false"
    if mime_type:
        filtro += f" and mimeType = '{mime_type}'"
    encontrados: list[dict] = []
    pendentes: list[tuple[str, list[str]]] = [(raiz_id, [])]
    while pendentes:
        pasta_id, caminho = pendentes.pop(0)
        for arquivo in _lista(drive, f"{filtro} and '{pasta_id}' in parents"):
            encontrados.append({**arquivo, "pasta_id": pasta_id, "caminho": caminho})
        subpastas = _lista(drive, f"mimeType = '{DRIVE_FOLDER_MIME}' and '{pasta_id}' in parents and trashed = false")
        pendentes.extend((p["id"], [*caminho, p["name"]]) for p in subpastas)
    return encontrados


def link_pasta(pasta_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{pasta_id}"


def upload_bytes(
    drive: Resource,
    pasta_id: str,
    nome_arquivo: str,
    conteudo: bytes,
    mime_type: str,
    converter_para: str | None = None,
) -> dict:
    """Sobe `conteudo` como arquivo novo em pasta_id. Com converter_para (ex.:
    DRIVE_DOC_MIME), o Drive converte na hora pro formato nativo do Google (um .docx
    vira Google Doc). Devolve {id, name, mimeType, webViewLink}."""
    media = MediaIoBaseUpload(io.BytesIO(conteudo), mimetype=mime_type, resumable=False)
    metadata = {"name": nome_arquivo, "parents": [pasta_id]}
    if converter_para:
        metadata["mimeType"] = converter_para
    arquivo = drive.files().create(
        body=metadata, media_body=media, fields=_CAMPOS_ARQUIVO, supportsAllDrives=True
    ).execute()
    print(f"[DRIVE] '{nome_arquivo}' publicado.")
    return arquivo
