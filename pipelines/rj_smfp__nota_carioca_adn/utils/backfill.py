"""Backfill do histórico a partir do Oracle (somente leitura) para o raw e o índice.

Lê ``NFSE_RPE.MV_DOCUMENTOSFISCAISNACIONAIS`` e a ``_QUARENTENA`` por faixa de NSU e grava os objetos no mesmo formato
do extrator (``origem='oracle_backfill'``, com ``quarentena_oracle`` em cada linha). É idempotente por NSU: faixas já
indexadas (de qualquer origem) não são relidas. Só lê dentro de ``janela_horaria`` (São Paulo) e no máximo
``linhas_por_minuto``; fora da janela termina sem tocar no Oracle e registra ``fora_da_janela``. A conferência com o ADN
direto está em ``utils.conferencia``.
"""

from dataclasses import dataclass
from typing import Literal

from pipelines.rj_smfp__nota_carioca_adn.constants import ORIGEM_ORACLE_BACKFILL
from pipelines.rj_smfp__nota_carioca_adn.utils.conferencia import RelatorioConferencia, conferir
from pipelines.rj_smfp__nota_carioca_adn.utils.percurso import ConfigBackfill, Percurso, ServicosBackfill
from pipelines.rj_smfp__nota_carioca_adn.utils.publicacao import Publicador
from pipelines.rj_smfp__nota_carioca_adn.utils.registro_oracle import registro_do_oracle

__all__ = [
    "ConfigBackfill",
    "RelatorioConferencia",
    "ResultadoBackfill",
    "ServicosBackfill",
    "conferir",
    "executar_backfill",
]


@dataclass(frozen=True, slots=True)
class ResultadoBackfill:
    """O que um backfill fez.

    :param faixas: Faixas processadas.
    :param linhas_lidas: Linhas completas (com CLOB) lidas do Oracle.
    :param linhas_gravadas: Linhas novas gravadas no raw e no índice.
    :param linhas_puladas: NSUs do Oracle que já estavam indexados.
    :param ultimo_nsu: Fim da última faixa processada; ``None`` se nenhuma.
    :param motivo_fim: ``concluido`` ou ``fora_da_janela``.
    """

    faixas: int
    linhas_lidas: int
    linhas_gravadas: int
    linhas_puladas: int
    ultimo_nsu: int | None
    motivo_fim: Literal["concluido", "fora_da_janela"]


def executar_backfill(config: ConfigBackfill, servicos: ServicosBackfill) -> ResultadoBackfill:
    """Copia do Oracle para o raw e o índice as linhas ainda não indexadas da faixa, dentro da janela horária.

    :param config: Faixa, janela e ritmo.
    :param servicos: Fonte, bucket, índice e relógio.
    :returns: Contagens e o motivo do fim (``concluido`` ou ``fora_da_janela``).
    """
    percurso = Percurso(config, servicos.relogio)
    publicador = Publicador(
        servicos.armazenamento, servicos.indice, config.ambiente, ORIGEM_ORACLE_BACKFILL, servicos.relogio
    )
    faixas = lidas = gravadas = puladas = 0
    ultimo: int | None = None
    preparado = False
    for faixa in percurso.faixas_liberadas():
        if not preparado:
            servicos.indice.garantir_tabelas()
            preparado = True
        nsus = servicos.fonte.nsus_na_faixa(faixa.ini, faixa.fim)
        pendentes = set(nsus) - servicos.indice.nsus_indexados(config.ambiente, faixa.ini, faixa.fim)
        puladas += len(nsus) - len(pendentes)
        if pendentes:
            linhas = servicos.fonte.ler_faixa(faixa.ini, faixa.fim)
            percurso.linhas_da_faixa = len(linhas)
            lidas += len(linhas)
            novas = [linha for linha in linhas if linha.nsu in pendentes]
            for inicio in range(0, len(novas), config.linhas_por_objeto):
                bloco = novas[inicio : inicio + config.linhas_por_objeto]
                publicador.publicar([registro_do_oracle(linha) for linha in bloco])
                gravadas += len(bloco)
        faixas, ultimo = faixas + 1, faixa.fim
    return ResultadoBackfill(faixas, lidas, gravadas, puladas, ultimo, percurso.motivo_fim)
