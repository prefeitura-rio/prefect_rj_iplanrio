"""Page-classification category constants and normalization for the Gemini classifier."""

# Valid Page Classification Categories (must match prompt v3)
# Aligned with extraction categories - no more "Nota Fiscal" generic, now specific types
PAGE_CATEGORIES = [
    "NF-e",  # Nota Fiscal Eletrônica (produtos/mercadorias)
    "NFS-e",  # Nota Fiscal de Serviços Eletrônica
    "NFST",  # Nota Fiscal de Serviços de Telecomunicações
    "DANFE",  # Documento Auxiliar da Nota Fiscal Eletrônica
    "Fatura",  # Qualquer fatura de serviço periódico (energia, gás, água, telefonia, locação, etc)
    "Nota de Débito",  # Vale-alimentação/refeição (Ticket, Sodexo, etc)
    "Nota de Cobrança",  # Cobrança formal por serviço prestado
    "Nota Fiscal de Locação de Bens Móveis",  # Locação de bens móveis
    "Nenhuma das Opções",  # Página sem documento fiscal
]

# Categories that are considered NF (Nota Fiscal) documents
# ALL categories except "Nenhuma das Opções" are considered fiscal documents
NF_CATEGORIES = [
    "NF-e",
    "NFS-e",
    "NFST",
    "DANFE",
    "Fatura",
    "Nota de Débito",
    "Nota de Cobrança",
    "Nota Fiscal de Locação de Bens Móveis",
]

# Category aliases for robust matching (handles typos and variations)
CATEGORY_ALIASES = {
    # NF-e variations
    "nf-e": "NF-e",
    "nfe": "NF-e",
    "nota fiscal eletronica": "NF-e",
    "nota fiscal eletrônica": "NF-e",
    "nota fiscal de produtos": "NF-e",
    "nota fiscal de mercadorias": "NF-e",
    "nf eletronica": "NF-e",
    "nf eletrônica": "NF-e",
    # NFS-e variations
    "nfs-e": "NFS-e",
    "nfse": "NFS-e",
    "nota fiscal de serviços": "NFS-e",
    "nota fiscal de servicos": "NFS-e",
    "nota fiscal servicos": "NFS-e",
    "nota fiscal serviços": "NFS-e",
    "nf de serviços": "NFS-e",
    "nf de servicos": "NFS-e",
    "nota fiscal eletronica de serviços": "NFS-e",
    "nota fiscal eletrônica de serviços": "NFS-e",
    # NFST variations
    "nfst": "NFST",
    "nota fiscal de serviços de telecomunicações": "NFST",
    "nota fiscal de servicos de telecomunicacoes": "NFST",
    "nf de telecomunicações": "NFST",
    "nf de telecomunicacoes": "NFST",
    "nf telecomunicações": "NFST",
    "nf telecomunicacoes": "NFST",
    "nota fiscal telecomunicações": "NFST",
    "nota fiscal telecomunicacoes": "NFST",
    "nf serviços telecomunicações": "NFST",
    "nf servicos telecomunicacoes": "NFST",
    "nota fiscal de serviço de telecomunicações": "NFST",
    "nota fiscal de servico de telecomunicacoes": "NFST",
    "nota fiscal de serviços de comunicação": "NFST",
    "nota fiscal de servicos de comunicacao": "NFST",
    # DANFE variations
    "danfe": "DANFE",
    "dafe": "DANFE",
    "danf": "DANFE",
    "documento auxiliar": "DANFE",
    "documento auxiliar da nota fiscal": "DANFE",
    "documento auxiliar da nf-e": "DANFE",
    # Fatura variations (genérica - todas as faturas)
    "fatura": "Fatura",
    "fatura generica": "Fatura",
    "fatura genérica": "Fatura",
    "invoice": "Fatura",
    "conta": "Fatura",
    "boleto": "Fatura",
    # Energia
    "fatura light": "Fatura",
    "light": "Fatura",
    "ligth": "Fatura",
    "energia light": "Fatura",
    "conta de luz": "Fatura",
    "conta luz": "Fatura",
    "fatura de energia": "Fatura",
    "conta de energia": "Fatura",
    "energia": "Fatura",
    "eletricidade": "Fatura",
    # Gás
    "fatura ceg": "Fatura",
    "ceg": "Fatura",
    "gás ceg": "Fatura",
    "gas ceg": "Fatura",
    "conta de gás": "Fatura",
    "conta de gas": "Fatura",
    "conta gas": "Fatura",
    "conta gás": "Fatura",
    "fatura de gás": "Fatura",
    "fatura de gas": "Fatura",
    "gás": "Fatura",
    "gas": "Fatura",
    # Água/Esgoto
    "fatura rioáguas": "Fatura",
    "fatura rioaguas": "Fatura",
    "rioáguas": "Fatura",
    "rioaguas": "Fatura",
    "rio águas": "Fatura",
    "rio aguas": "Fatura",
    "cedae": "Fatura",
    "água e esgoto": "Fatura",
    "agua e esgoto": "Fatura",
    "conta de água": "Fatura",
    "conta de agua": "Fatura",
    "conta água": "Fatura",
    "conta agua": "Fatura",
    "fatura de água": "Fatura",
    "fatura de agua": "Fatura",
    "água": "Fatura",
    "agua": "Fatura",
    "esgoto": "Fatura",
    # Locação
    "fatura de locação": "Fatura",
    "fatura de locacao": "Fatura",
    "locação": "Fatura",
    "locacao": "Fatura",
    "aluguel": "Fatura",
    "recibo de aluguel": "Fatura",
    "fatura locação": "Fatura",
    "fatura locacao": "Fatura",
    # Telefonia
    "fatura telefonia": "Fatura",
    "telefonia": "Fatura",
    "telefone": "Fatura",
    "telecomunicações": "Fatura",
    "telecomunicacoes": "Fatura",
    "claro": "Fatura",
    "vivo": "Fatura",
    "tim": "Fatura",
    "oi": "Fatura",
    "fatura telefone": "Fatura",
    "conta telefone": "Fatura",
    "conta de telefone": "Fatura",
    "fatura de telefone": "Fatura",
    "fatura de telefonia": "Fatura",
    # Aérea
    "fatura aérea": "Fatura",
    "fatura aerea": "Fatura",
    "rede aérea": "Fatura",
    "rede aerea": "Fatura",
    # Nota de Débito variations
    "nota de debito": "Nota de Débito",
    "nota de débito": "Nota de Débito",
    "nota débito": "Nota de Débito",
    "ticket": "Nota de Débito",
    "sodexo": "Nota de Débito",
    "alelo": "Nota de Débito",
    "vr": "Nota de Débito",
    "flash": "Nota de Débito",
    "vale alimentação": "Nota de Débito",
    "vale alimentacao": "Nota de Débito",
    "vale refeição": "Nota de Débito",
    "vale refeicao": "Nota de Débito",
    "vale-alimentação": "Nota de Débito",
    "vale-refeição": "Nota de Débito",
    # Nota de Cobrança variations
    "nota de cobranca": "Nota de Cobrança",
    "nota de cobrança": "Nota de Cobrança",
    "nota cobrança": "Nota de Cobrança",
    "cobrança": "Nota de Cobrança",
    "cobranca": "Nota de Cobrança",
    # Nenhuma das Opções variations
    "nenhuma das opções": "Nenhuma das Opções",
    "nenhuma das opcoes": "Nenhuma das Opções",
    "nenhuma": "Nenhuma das Opções",
    "nenhum": "Nenhuma das Opções",
    "página em branco": "Nenhuma das Opções",
    "pagina em branco": "Nenhuma das Opções",
    "em branco": "Nenhuma das Opções",
    "não identificado": "Nenhuma das Opções",
    "nao identificado": "Nenhuma das Opções",
    "outro": "Nenhuma das Opções",
    "outros": "Nenhuma das Opções",
    "desconhecido": "Nenhuma das Opções",
    # Portal da Nota Fiscal Eletrônica (printout/consulta, not a valid fiscal document)
    "portal da nota fiscal": "Nenhuma das Opções",
    "portal da nota fiscal eletrônica": "Nenhuma das Opções",
    "portal da nota fiscal eletronica": "Nenhuma das Opções",
    "portal nf-e": "Nenhuma das Opções",
    "portal nfe": "Nenhuma das Opções",
    "consulta nf-e": "Nenhuma das Opções",
    "consulta nfe": "Nenhuma das Opções",
    "consulta nota fiscal": "Nenhuma das Opções",
    "print nota fiscal": "Nenhuma das Opções",
}


def normalize_category(raw_category: str) -> str:
    """
    Normalize a category string to a valid PAGE_CATEGORIES value.
    Handles typos, variations, and case differences.

    :param raw_category: Raw category string from model response.
    :returns: Normalized category from PAGE_CATEGORIES, or
        "Nenhuma das Opções" if not matched.
    """
    if not raw_category:
        return "Nenhuma das Opções"

    # Clean and lowercase for comparison
    cleaned = raw_category.strip().lower()

    # Direct match (case-insensitive)
    for valid_cat in PAGE_CATEGORIES:
        if cleaned == valid_cat.lower():
            return valid_cat

    # Check aliases
    if cleaned in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[cleaned]

    # Partial match - check if any alias is contained in the response
    for alias, valid_cat in CATEGORY_ALIASES.items():
        if alias in cleaned or cleaned in alias:
            return valid_cat

    # Fuzzy match using similarity (Levenshtein-like)
    best_match = None
    best_score = 0

    for valid_cat in PAGE_CATEGORIES:
        score = similarity_score(cleaned, valid_cat.lower())
        if score > best_score and score > 0.7:  # Threshold of 70% similarity
            best_score = score
            best_match = valid_cat

    if best_match:
        return best_match

    # Default fallback
    return "Nenhuma das Opções"


def similarity_score(s1: str, s2: str) -> float:
    """
    Calculate similarity between two strings (simple ratio).
    Returns value between 0.0 and 1.0.
    """
    if not s1 or not s2:
        return 0.0

    # Simple character overlap ratio
    s1_set = set(s1.lower())
    s2_set = set(s2.lower())

    intersection = len(s1_set & s2_set)
    union = len(s1_set | s2_set)

    if union == 0:
        return 0.0

    jaccard = intersection / union

    # Also consider length similarity
    len_ratio = min(len(s1), len(s2)) / max(len(s1), len(s2))

    # Combined score
    return (jaccard + len_ratio) / 2


def is_nf_category(category: str) -> bool:
    """
    Check if a category represents a NF (Nota Fiscal) document.

    :param category: Normalized category string.
    :returns: True if the category is a type of NF/invoice.
    """
    normalized = normalize_category(category)
    return normalized in NF_CATEGORIES
