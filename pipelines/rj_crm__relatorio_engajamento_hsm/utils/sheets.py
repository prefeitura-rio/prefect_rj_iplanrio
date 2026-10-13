# -*- coding: utf-8 -*-
"""Helpers de Google Sheets — ler uma aba como DataFrame, publicar um DataFrame como
Sheet nativo numa pasta do Drive e atualizar células de uma linha pelo nome da coluna.

Toda escrita usa valueInputOption=RAW: o Sheets grava o texto como veio, sem
interpretar — com USER_ENTERED, um id_sessao como '123e4567' viraria número em notação
científica e uma fala do cidadão começando com '=' viraria fórmula.

Toda leitura usa o valor FORMATADO (default da API) — é o mesmo texto que a tabela
externa do BigQuery lê do Sheet, o que permite casar uma linha lida pelo BQ com a
linha do Sheet (ver atualiza_linha)."""

from __future__ import annotations

import pandas as pd
from googleapiclient.discovery import Resource, build
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_crm__relatorio_engajamento_hsm.config import DRIVE_SHEET_MIME, SHEETS_MAX_CHARS_CELULA
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.drive import busca_arquivo

COL_LINHA = "_linha"  # nº da linha no Sheet (1 = cabeçalho), incluída por le_sheet(com_linha=True)

_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_sheets_service() -> Resource:
    credentials = get_bd_credentials_from_env(mode="prod", scopes=_SCOPES)
    return build("sheets", "v4", credentials=credentials)


def _coluna_a1(indice: int) -> str:
    """Índice 0-based -> letra(s) de coluna A1 (0 -> A, 25 -> Z, 26 -> AA)."""
    letras = ""
    indice += 1
    while indice:
        indice, resto = divmod(indice - 1, 26)
        letras = chr(ord("A") + resto) + letras
    return letras


def _range_aba(aba: str, celulas: str = "") -> str:
    aba_escapada = aba.replace("'", "''")
    return f"'{aba_escapada}'!{celulas}" if celulas else f"'{aba_escapada}'"


def primeira_aba(sheets: Resource, spreadsheet_id: str) -> str:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title").execute()
    return meta["sheets"][0]["properties"]["title"]


def le_sheet(sheets: Resource, spreadsheet_id: str, aba: str | None = None, com_linha: bool = False) -> pd.DataFrame:
    """Aba inteira como DataFrame de strings (1ª linha = cabeçalho). Linhas mais curtas
    que o cabeçalho (a API corta as células vazias do fim) são completadas com "";
    linhas totalmente vazias são descartadas. Sem `aba`, lê a primeira."""
    aba = aba or primeira_aba(sheets, spreadsheet_id)
    valores = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=_range_aba(aba)
    ).execute().get("values", [])
    if not valores:
        return pd.DataFrame()

    cabecalho = [str(c).strip() for c in valores[0]]
    linhas, numeros = [], []
    for n, bruta in enumerate(valores[1:], start=2):
        linha = [str(v) for v in bruta[: len(cabecalho)]]
        linha += [""] * (len(cabecalho) - len(linha))
        if any(v.strip() for v in linha):
            linhas.append(linha)
            numeros.append(n)

    df = pd.DataFrame(linhas, columns=cabecalho)
    if com_linha:
        df[COL_LINHA] = numeros
    return df


def _para_valores(df: pd.DataFrame) -> list[list[str]]:
    def _celula(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        texto = str(v)
        if len(texto) > SHEETS_MAX_CHARS_CELULA:
            texto = texto[: SHEETS_MAX_CHARS_CELULA - 10] + " […]"
        return texto

    return [[str(c) for c in df.columns]] + [[_celula(v) for v in linha] for linha in df.itertuples(index=False)]


def _escreve(sheets: Resource, spreadsheet_id: str, aba: str, df: pd.DataFrame) -> None:
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=_range_aba(aba, "A1"),
        valueInputOption="RAW",
        body={"values": _para_valores(df)},
    ).execute()


def cria_sheet(drive: Resource, sheets: Resource, pasta_id: str, nome: str, df: pd.DataFrame) -> dict:
    """Cria um Google Sheet nativo `nome` em pasta_id com o conteúdo de df (cabeçalho
    na 1ª linha). Criado pelo Drive (não pela API do Sheets) porque só o Drive deixa
    escolher a pasta. Devolve {id, name, mimeType, webViewLink}."""
    arquivo = drive.files().create(
        body={"name": nome, "mimeType": DRIVE_SHEET_MIME, "parents": [pasta_id]},
        fields="id, name, mimeType, webViewLink",
        supportsAllDrives=True,
    ).execute()
    _escreve(sheets, arquivo["id"], primeira_aba(sheets, arquivo["id"]), df)
    log(f"[SHEETS] '{nome}' criado ({len(df)} linha(s)).")
    return arquivo


def substitui_sheet(sheets: Resource, spreadsheet_id: str, df: pd.DataFrame) -> None:
    """Apaga o conteúdo da 1ª aba e grava df no lugar — mantém o mesmo arquivo (ID,
    link e compartilhamento). Substituir em vez de apagar+recriar também evita depender
    de permissão de exclusão na Unidade Compartilhada (Editor/Colaborador não tem)."""
    aba = primeira_aba(sheets, spreadsheet_id)
    sheets.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=_range_aba(aba), body={}).execute()
    _escreve(sheets, spreadsheet_id, aba, df)


def publica_sheet(drive: Resource, sheets: Resource, pasta_id: str, nome: str, df: pd.DataFrame) -> dict:
    """Cria o Sheet `nome` em pasta_id, ou substitui o conteúdo se já existir um com
    esse nome exato. Devolve {id, name, mimeType, webViewLink}."""
    existente = busca_arquivo(drive, nome, pasta_id, mime_type=DRIVE_SHEET_MIME)
    if existente is None:
        return cria_sheet(drive, sheets, pasta_id, nome, df)
    substitui_sheet(sheets, existente["id"], df)
    log(f"[SHEETS] '{nome}' substituído ({len(df)} linha(s)).")
    return existente


def atualiza_linha(sheets: Resource, spreadsheet_id: str, aba: str, linha: int, valores: dict[str, str]) -> None:
    """Grava `valores` ({nome da coluna: valor}) na linha `linha` do Sheet, achando a
    coluna pelo cabeçalho — não pela posição, pra não quebrar se alguém mover/inserir
    coluna. Coluna inexistente no cabeçalho é erro (melhor falhar alto do que gravar
    status no lugar errado)."""
    cabecalho = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=_range_aba(aba, "1:1")
    ).execute().get("values", [[]])[0]
    posicao = {str(c).strip(): i for i, c in enumerate(cabecalho)}

    faltando = [col for col in valores if col not in posicao]
    if faltando:
        raise ValueError(
            f"Coluna(s) {faltando} não encontrada(s) no cabeçalho da aba '{aba}' do Sheet {spreadsheet_id}."
        )

    dados = [
        {"range": _range_aba(aba, f"{_coluna_a1(posicao[col])}{linha}"), "values": [[valor]]}
        for col, valor in valores.items()
    ]
    sheets.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"valueInputOption": "RAW", "data": dados}
    ).execute()
