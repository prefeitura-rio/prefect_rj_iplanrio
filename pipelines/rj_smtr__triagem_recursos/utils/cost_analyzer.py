"""
Módulo de Auditoria Financeira, Consumo de Tokens e Eficiência de Custo.

Calcula custos reais e projetados para lotes de teste e escala operacional (1k, 10k processos),
aplicando descontos de Prompt Caching e computando índices de Custo por Acerto.
"""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Dict, List
import pandas as pd


@dataclass
class TabelaPrecoModelo:
    nome_modelo: str
    preco_in_1m: float
    preco_out_1m: float
    desconto_cache: float = 0.90
    indice_inteligencia: int = 0

    @property
    def preco_cache_in_1m(self) -> float:
        return self.preco_in_1m * (1.0 - self.desconto_cache)


TABELA_PRECOS_PADRAO: Dict[str, TabelaPrecoModelo] = {
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
    aplicar_cache_estatico: bool = True
) -> Dict[str, float]:
    """Calcula o custo exato em USD para uma única inferência."""
    tabela = TABELA_PRECOS_PADRAO.get(modelo.lower())
    if not tabela:
        # Fallback genérico se o nome não for mapeado exatamente
        tabela = TabelaPrecoModelo(nome_modelo=modelo, preco_in_1m=0.50, preco_out_1m=2.00)

    # Se aplicar cache estático no prompt do sistema (~1.5k tokens)
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
        "economia_cache_usd": round(custo_sem_cache - custo_total, 6)
    }


def gerar_relatorio_comparativo_modelos(
    caminhos_resultados: List[Path],
    taxa_cambio_brl: float = 5.80
) -> pd.DataFrame:
    """Consolida múltiplos DataFrames de execução e gera matriz de custo-benefício."""
    linhas_resumo = []

    for caminho in caminhos_resultados:
        if not caminho.exists():
            continue

        df = pd.read_csv(caminho)
        if df.empty:
            continue

        modelo = str(df["modelo"].iloc[0])
        total_casos = len(df)
        acuracia = float(df["acertou_hipotese"].mean() * 100) if "acertou_hipotese" in df else 0.0

        # Médias de tokens (se registradas, ou estimativas padrão: 3.5k in / 150 out)
        avg_prompt_tokens = int(df["prompt_tokens"].mean()) if "prompt_tokens" in df else 3500
        avg_comp_tokens = int(df["completion_tokens"].mean()) if "completion_tokens" in df else 150

        custo_unitario = calcular_custo_chamada(
            modelo=modelo,
            prompt_tokens=avg_prompt_tokens,
            completion_tokens=avg_comp_tokens,
            aplicar_cache_estatico=True
        )

        custo_amostra_usd = custo_unitario["custo_total_usd"] * total_casos
        custo_1k_usd = custo_unitario["custo_total_usd"] * 1_000
        custo_1k_brl = custo_1k_usd * taxa_cambio_brl

        # Custo por caso correto
        custo_por_acerto_usd = (custo_1k_usd / (acuracia * 10)) if acuracia > 0 else 0.0

        tabela_info = TABELA_PRECOS_PADRAO.get(modelo.lower())
        intel_score = tabela_info.indice_inteligencia if tabela_info else 0

        linhas_resumo.append({
            "Modelo": modelo,
            "Intelligence": intel_score,
            "Acurácia (%)": round(acuracia, 2),
            "Avg In Tokens": avg_prompt_tokens,
            "Avg Out Tokens": avg_comp_tokens,
            f"Custo Lote ({total_casos})": f"${custo_amostra_usd:.4f}",
            "Custo / 1k (USD)": f"${custo_1k_usd:.2f}",
            "Custo / 1k (BRL)": f"R$ {custo_1k_brl:.2f}",
            "Custo / Acerto (1k)": f"${custo_por_acerto_usd:.4f}"
        })

    df_comparativo = pd.DataFrame(linhas_resumo)

    #print("\n" + "=" * 80)
    #print("MATRIZ COMPARATIVA DE CUSTO, CONSUMO E EFICIÊNCIA")
    #print("=" * 80)
    #print(df_comparativo.to_string(index=False))
    #print("=" * 80 + "\n")

    return df_comparativo