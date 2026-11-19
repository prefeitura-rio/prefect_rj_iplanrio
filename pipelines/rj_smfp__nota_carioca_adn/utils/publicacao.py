"""Publicação de um lote: objeto imutável no bucket e linhas no índice, nessa ordem, de forma idempotente."""

from collections.abc import Sequence

from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import Armazenamento, ObjetoJaExiste, gravar_imutavel
from pipelines.rj_smfp__nota_carioca_adn.utils.indice import Indice
from pipelines.rj_smfp__nota_carioca_adn.utils.lote import Ingestao, linhas_do_indice, montar_lote
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import RegistroDocumento
from pipelines.rj_smfp__nota_carioca_adn.utils.relogio import Relogio
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


class ObjetoDivergente(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """Já existe um objeto com o nome do lote e outro conteúdo: o ADN mudou um NSU ou o lote foi remontado diferente."""


class Publicador:
    """Grava lotes do raw e do índice de um ambiente e de uma origem.

    Uma falha entre o objeto e o índice, ou entre o índice e o cursor, é refeita sem duplicar: o objeto igual já
    existente é reaproveitado e só os NSUs ainda não indexados entram no índice.

    :param armazenamento: Bucket do raw.
    :param indice: Índice no BigQuery (carga por load job).
    :param ambiente: ``staging`` ou ``prod``.
    :param origem: ``adn`` ou ``oracle_backfill``.
    :param relogio: Hora da ingestão.
    """

    def __init__(
        self, armazenamento: Armazenamento, indice: Indice, ambiente: str, origem: str, relogio: Relogio
    ) -> None:
        self._armazenamento = armazenamento
        self._indice = indice
        self._ambiente = ambiente
        self._origem = origem
        self._relogio = relogio

    def publicar(self, registros: Sequence[RegistroDocumento]) -> int:
        """Grava o objeto e indexa os NSUs que ainda não estão no índice.

        :returns: Linhas novas no índice.
        :raises ObjetoDivergente: Se o objeto existe com outro conteúdo (nada é sobrescrito).
        """
        lote = montar_lote(self._ambiente, registros)
        try:
            gravar_imutavel(self._armazenamento, lote.objeto, lote.conteudo)
        except ObjetoJaExiste:
            if self._armazenamento.ler(lote.objeto) != lote.conteudo:
                raise ObjetoDivergente(f"{lote.objeto} já existe com outro conteúdo; nada foi sobrescrito") from None
            logger.info("Objeto %s já existia com o mesmo conteúdo; reaproveitado", lote.objeto)
        existentes = self._indice.nsus_indexados(self._ambiente, lote.nsu_ini, lote.nsu_fim)
        novas = [
            linha
            for linha in linhas_do_indice(
                lote,
                self._armazenamento.uri(lote.objeto),
                Ingestao(self._origem, self._ambiente, self._relogio.agora_utc()),
            )
            if linha.nsu not in existentes
        ]
        if novas:
            self._indice.carregar_documentos(novas)
        return len(novas)
