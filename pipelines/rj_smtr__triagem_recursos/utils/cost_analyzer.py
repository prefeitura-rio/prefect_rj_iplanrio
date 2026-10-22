"""Módulo de Auditoria Financeira, Consumo de Tokens e Eficiência de Custo.

Calcula custos reais e projetados para lotes de teste e escala operacional (1k, 10k processos),
aplicando descontos de Prompt Caching e computando índices de Custo por Acerto.
"""

from dataclasses import dataclass
import os
from pathlib import Path
import pandas as pd


@dataclass
class TabelaPrecoModelo:
    """Tabela de tarifação oficial por milhão de tokens."""

    nome_modelo: str
    preco_in_1m: float
    preco_out_1m: float
    desconto_cache: float = 0.90
    indice_inteligencia: int = 0

    @property
    def preco_cache_in_1m(self) -> float:
        """Retorna o valor efetivo de entrada sob cache implícito/explícito."""
        return self.preco_in_1m * (1.0 - self.desconto_cache)


TABELA_PRECOS_PADRAO: dict[str, TabelaPrecoModelo] = {
    "gemini-3.7-flash": TabelaPrecoModelo(
        nome_modelo="gemini-3.7-flash",
        preco_in_1m=float(os.getenv("PRICE_GEMINI_3_7_FLASH_IN", 0.75)),
        preco_out_1m=float(os.getenv("PRICE_GEMINI_3_7_FLASH_OUT", 3.75)),
        desconto_cache=float(os.getenv("PRICE_GEMINI_3_7_FLASH_CACHE_DISCOUNT", 0.90)),
        indice_inteligencia=int(os.getenv("INTEL_GEMINI_3_7_FLASH", 53)),
    ),
    "gemini-3.5-flash-lite": TabelaPrecoModelo(
        nome_modelo="gemini-3.5-flash-lite",
        preco_in_1m=float(os.getenv("PRICE_GEMINI_3_5_FLASH_LITE_IN", 0.30)),
        preco_out_1m=float(os.getenv("PRICE_GEMINI_3_5_FLASH_LITE_OUT", 2.50)),
        desconto_cache=float(os.getenv("PRICE_GEMINI_3_5_FLASH_LITE_CACHE_DISCOUNT", 0.90)),
        indice_inteligencia=int(os.getenv("INTEL_GEMINI_3_5_FLASH_LITE", 37)),
    ),
}


def calcular_custo_chamada(
    modelo: str,
    prompt_tokens: int,
    completion_tokens: int,
    cached_tokens: int = 0,
    aplicar_cache_estatico: bool = True,
) -> dict[str, float]:
    """Calcula o custo exato em USD para uma única inferência.

    :param modelo: Identificador do modelo na API.
    :param prompt_tokens: Total de tokens no prompt de entrada.
    :param completion_tokens: Total de tokens gerados na saída.
    :param cached_tokens: Quantidade de tokens aproveitados do cache.
    :param aplicar_cache_estatico: Flag para aplicar desconto no prompt de sistema padrão.
    :returns: Dicionário contendo custo_total_usd, custo_sem_cache_usd e economia_cache_usd.
    """
    mod_clean = modelo.lower().replace("vertex/", "").replace("google/", "")
    tabela = TABELA_PRECOS_PADRAO.get(mod_clean)
    if not tabela:
        tabela = TabelaPrecoModelo(
            nome_modelo=modelo, preco_in_1m=0.75, preco_out_1m=3.75
        )

    if aplicar_cache_estatico and cached_tokens == 0 and prompt_tokens > 1500:
        tokens_cache = 1500
        tokens_in_puro = max(0, prompt_tokens - 1500)
    else:
        tokens_cache = cached_tokens
        tokens_in_puro = max(0, prompt_tokens - cached_tokens)

    custo_in_puro = (tokens_in_puro / 1_000_000) * tabela.preco_in_1m
    custo_in_cache = (tokens_cache / 1_000_000) * tabela.preco_cache_in_1m
    custo_out = (completion_tokens / 1_000_000) * tabela.preco_out_1m

    custo_total = custo_in_puro + custo_in_cache + custo_out
    custo_sem_cache = ((prompt_tokens / 1_000_000) * tabela.preco_in_1m) + custo_out

    return {
        "custo_total_usd": round(custo_total, 6),
        "custo_sem_cache_usd": round(custo_sem_cache, 6),
        "economia_cache_usd": round(custo_sem_cache - custo_total, 6),
    }


def gerar_relatorio_comparativo_modelos(
    caminhos_resultados: list[Path],
    taxa_cambio_brl: float = 5.80,
) -> pd.DataFrame:
    """Consolida múltiplos DataFrames de execução e gera matriz de custo-benefício.

    :param caminhos_resultados: Lista de caminhos para arquivos CSV de execução.
    :param taxa_cambio_brl: Cotação de conversão cambial USD/BRL.
    :returns: DataFrame com resumo comparativo consolidado.
    """
    linhas_resumo = []

    for caminho in caminhos_resultados:
        if not caminho.exists():
            continue

        df = pd.read_csv(caminho)
        if df.empty:
            continue

        modelo = str(df["modelo"].iloc[0])
        total_casos = len(df)
        acuracia = (
            float(df["acertou_hipotese"].mean() * 100)
            if "acertou_hipotese" in df
            else 0.0
        )

        media_tokens_in = (
            float(df["tokens_input"].mean())
            if "tokens_input" in df
            else 3500.0
        )
        media_tokens_out = (
            float(df["tokens_output"].mean())
            if "tokens_output" in df
            else 150.0
        )
        media_tokens_cache = (
            float(df["tokens_cached"].mean())
            if "tokens_cached" in df
            else 0.0
        )

        info_custo = calcular_custo_chamada(
            modelo=modelo,
            prompt_tokens=int(media_tokens_in),
            completion_tokens=int(media_tokens_out),
            cached_tokens=int(media_tokens_cache),
        )

        custo_unit_usd = info_custo["custo_total_usd"]
        custo_unit_brl = custo_unit_usd * taxa_cambio_brl

        custo_acerto = custo_unit_usd / (acuracia / 100.0) if acuracia > 0 else 0.0

        linhas_resumo.append({
            "Modelo": modelo,
            "Acurácia Global (%)": round(acuracia, 2),
            "Tokens In Médio": int(media_tokens_in),
            "Tokens Out Médio": int(media_tokens_out),
            "Tokens Cache Médio": int(media_tokens_cache),
            "Custo Unitário (USD)": f"${custo_unit_usd:.5f}",
            "Custo Unitário (BRL)": f"R${custo_unit_brl:.4f}",
            "Custo Lote 760 (BRL)": f"R${custo_unit_brl * 760:.2f}",
            "Projeção 2.000 (BRL)": f"R${custo_unit_brl * 2000:.2f}",
            "Custo por Acerto (USD)": f"${custo_acerto:.5f}",
            "Economia Cache Unit. (USD)": f"${info_custo['economia_cache_usd']:.5f}",
        })

    return pd.DataFrame(linhas_resumo)
