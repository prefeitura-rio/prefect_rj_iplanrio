# -*- coding: utf-8 -*-
"""Catálogo de categorias — só em memória (dura só o processamento de 1 disparo, não
persiste entre execuções: cada disparo é um catálogo novo, não precisa ficar estável
entre dias porque a extração já é 1x só, do início ao fim, num único flow run).
Compartilhado entre discovery (só quem registra categoria nova) e classify (só busca,
nunca cria)."""

from __future__ import annotations

from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import normaliza_rotulo


class CatalogoCategorias:
    def __init__(self):
        self._por_chave: dict[str, tuple[str, str]] = {}

    def registra(self, nome: str, descricao: str = "") -> str:
        """Registra (se nova) e retorna o nome CANÔNICO — usado como valor gravado,
        mesmo se a LLM mandou uma grafia levemente diferente. Só a descoberta chama
        isso — é a única que pode criar categoria."""
        chave = normaliza_rotulo(nome)
        if chave not in self._por_chave:
            self._por_chave[chave] = (nome.strip(), (descricao or "").strip())
        return self._por_chave[chave][0]

    def busca(self, nome: str) -> str | None:
        """Como `registra`, mas NUNCA cria — devolve o nome canônico se a categoria já
        existe, ou None. Usado pela classificação: ela só pode escolher entre
        categorias já existentes, criar é trabalho exclusivo da descoberta."""
        entrada = self._por_chave.get(normaliza_rotulo(nome))
        return entrada[0] if entrada else None

    def como_dict(self) -> dict[str, str]:
        return dict(self._por_chave.values())

    def texto_prompt(self) -> str:
        if not self._por_chave:
            return "(nenhuma categoria criada ainda — você pode criar as primeiras)"
        linhas = sorted(f"- {nome}: {descricao}" for nome, descricao in self._por_chave.values())
        return "\n".join(linhas)
