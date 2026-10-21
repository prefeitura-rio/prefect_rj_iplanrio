"""
Módulo de abstração de armazenamento para obtenção de PDFs e artefatos.
Desacoplado do GCS: suporta leitura em memória local ou via cliente SEI.
"""

from pathlib import Path
from typing import Any, Optional, Union

from ..constants import LOCAL_ARTIFACTS_DIR, LOCAL_PDFS_DIR


def carregar_bytes_pdf(
    identificador: str,
    fonte_pdf: Union[str, bool] = "local",
    cliente_sei: Optional[Any] = None,
    diretorio_local: Optional[Path] = None,
    is_local: Optional[bool] = None,
) -> bytes:
    """Lê bytes do PDF diretamente em memória a partir da pasta local ou via SEI.

    :param identificador: Nome do arquivo (modo local) ou id_documento_sei (modo SEI).
    :param fonte_pdf: "local" para ler de LOCAL_PDFS_DIR ou "sei" para chamar cliente_sei.
    :param cliente_sei: Instância do cliente SEI fornecendo baixar_documento().
    :param diretorio_local: Caminho opcional do diretório de PDFs locais.
    :param is_local: Compatibilidade retroativa com booleano is_local.
    :return: Stream de bytes do PDF em memória RAM.
    """
    # Tratamento defensivo de parâmetros legados
    if is_local is not None:
        modo = "local" if is_local else "sei"
    elif isinstance(fonte_pdf, bool):
        modo = "local" if fonte_pdf else "sei"
    else:
        modo = str(fonte_pdf).strip().lower()

    if modo == "local":
        pasta = diretorio_local or LOCAL_PDFS_DIR
        caminho = pasta / identificador
        if not caminho.exists():
            raise FileNotFoundError(
                f"PDF local não localizado em: {caminho.resolve()}"
            )
        return caminho.read_bytes()

    elif modo == "sei":
        if cliente_sei is None:
            raise ValueError(
                "O parâmetro 'cliente_sei' é obrigatório quando fonte_pdf='sei'."
            )
        if not hasattr(cliente_sei, "baixar_documento"):
            raise AttributeError(
                "O objeto 'cliente_sei' deve implementar o método 'baixar_documento(id_documento_sei)'."
            )
        return cliente_sei.baixar_documento(identificador)

    raise ValueError(
        f"Modo de fonte de PDF inválido: '{modo}'. Use 'local' ou 'sei'."
    )


def carregar_bytes_artefato(
    nome_artefato: str, diretorio_local: Optional[Path] = None
) -> bytes:
    """Lê bytes de um artefato estático localmente."""
    pasta = diretorio_local or LOCAL_ARTIFACTS_DIR
    caminho = pasta / nome_artefato
    if not caminho.exists():
        raise FileNotFoundError(f"Artefato local ausente: {caminho.resolve()}")
    return caminho.read_bytes()
