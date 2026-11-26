"""Mensagem do extrator no Discord, por ambiente: uma mensagem editada no lugar e um aviso final avulso.

Segue o padrão da carga BigQuery → Oracle: o webhook é o do ambiente (staging nunca usa o de prod, e sem webhook fica
calado), as mensagens de staging levam o marcador ``🧪 STAGING · `` e nada aqui levanta exceção para o run.
"""

import httpx

from pipelines.rj_smfp__nota_carioca_adn.utils.discord import (
    DiscordStatusMessage,
    Environment,
    never_raises,
    resolve_webhook,
)
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao

MARCADOR_STAGING = "🧪 STAGING · "
COR_ANDAMENTO = 3447003
COR_SUCESSO = 3066993
COR_FALHA = 15158332
LIMITE_ERRO = 500
SEM_MENCOES = {"parse": []}
MOTIVOS = {
    "sem_documentos": "sem documentos novos (o ADN pede esperar 1 h)",
    "limite_de_lotes": "parou no limite de lotes do run",
    "em_andamento": "em andamento",
}


class NotificadorNulo:
    """Não avisa ninguém (testes e execuções sem Discord)."""

    def iniciar(self, cursor: int) -> None:
        """Nada a fazer."""

    def progresso(self, resultado: ResultadoExtracao) -> None:
        """Nada a fazer."""

    def concluir(self, resultado: ResultadoExtracao) -> None:
        """Nada a fazer."""

    def falhar(self, erro: BaseException) -> None:
        """Nada a fazer."""


class ExtracaoNotificador:
    """Notificador do Discord do extrator.

    :param ambiente: Ambiente do run.
    :param mensagem: Cliente do webhook; ``None`` desliga o Discord.
    :param run_url: Link do flow run no Prefect, quando conhecido.
    """

    def __init__(self, ambiente: Environment, mensagem: DiscordStatusMessage | None, run_url: str | None) -> None:
        self._ambiente = ambiente
        self._mensagem = mensagem
        self._url = run_url
        self._cursor_inicial = 0

    @classmethod
    def criar(
        cls,
        ambiente: Environment,
        habilitado: bool = True,
        transporte: httpx.BaseTransport | None = None,
        run_url: str | None = None,
    ) -> "ExtracaoNotificador":
        """Cria o notificador com o webhook do ambiente (Infisical ou Secret block); sem webhook, fica calado."""
        mensagem = (
            DiscordStatusMessage(resolve_webhook(environment=ambiente), transport=transporte) if habilitado else None
        )
        return cls(ambiente, mensagem if mensagem is not None and mensagem.enabled else None, run_url)

    def _titulo(self, texto: str) -> str:
        marca = MARCADOR_STAGING if self._ambiente is Environment.STAGING else ""
        return f"{marca}{texto}"

    def _embed(self, titulo: str, cor: int, campos: dict[str, str], descricao: str = "") -> dict[str, object]:
        embed: dict[str, object] = {
            "title": self._titulo(titulo),
            "color": cor,
            "fields": [{"name": nome, "value": valor, "inline": True} for nome, valor in campos.items()],
        }
        if descricao:
            embed["description"] = descricao
        if self._url:
            embed["url"] = self._url
        return {"embeds": [embed], "allowed_mentions": SEM_MENCOES}

    def _campos(self, resultado: ResultadoExtracao) -> dict[str, str]:
        return {
            "Cursor": f"{self._cursor_inicial} → {resultado.cursor_final}",
            "Lotes": str(resultado.lotes),
            "Documentos": str(resultado.documentos),
            "Ilegíveis": str(resultado.ilegiveis),
            "Lacunas preenchidas": str(resultado.lacunas_preenchidas),
            "Lacunas inexistentes": str(resultado.lacunas_inexistentes),
        }

    def _publicar(self, payload: dict[str, object], forcar: bool) -> None:
        if self._mensagem is not None:
            self._mensagem.publish(payload, force=forcar)

    def _anunciar(self, texto: str) -> None:
        if self._mensagem is not None:
            self._mensagem.announce({"content": self._titulo(texto), "allowed_mentions": SEM_MENCOES})

    @never_raises
    def iniciar(self, cursor: int) -> None:
        """Cria a mensagem do run com o cursor de partida."""
        self._cursor_inicial = cursor
        self._publicar(self._embed("Extração ADN em andamento", COR_ANDAMENTO, {"Cursor": str(cursor)}), forcar=True)

    @never_raises
    def progresso(self, resultado: ResultadoExtracao) -> None:
        """Atualiza a mensagem (sem forçar: o cliente respeita o intervalo mínimo entre edições)."""
        self._publicar(self._embed("Extração ADN em andamento", COR_ANDAMENTO, self._campos(resultado)), forcar=False)

    @never_raises
    def concluir(self, resultado: ResultadoExtracao) -> None:
        """Finaliza em verde e envia o aviso avulso com os números do run."""
        descricao = f"Run encerrado: {MOTIVOS[resultado.motivo_fim]}."
        self._publicar(self._embed("Extração ADN concluída", COR_SUCESSO, self._campos(resultado), descricao), True)
        self._anunciar(
            f"✅ Extração ADN concluída: {resultado.documentos} documentos em {resultado.lotes} lotes, "
            f"cursor {resultado.cursor_final}."
        )

    @never_raises
    def falhar(self, erro: BaseException) -> None:
        """Finaliza em vermelho com o tipo e a mensagem do erro e envia o aviso avulso."""
        detalhe = f"{type(erro).__name__}: {erro}"[:LIMITE_ERRO]
        self._publicar(
            self._embed("Extração ADN falhou", COR_FALHA, {"Cursor inicial": str(self._cursor_inicial)}, detalhe), True
        )
        self._anunciar(f"❌ Extração ADN falhou: {detalhe}")
