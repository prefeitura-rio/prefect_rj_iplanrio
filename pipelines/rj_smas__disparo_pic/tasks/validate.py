import pandas as pd
from prefect import task


@task
def validate_data(
    df: pd.DataFrame, required_columns: list[str] | None = None
) -> pd.DataFrame:
    """
    Executa validações básicas no DataFrame:
    - Confere se colunas obrigatórias existem.
    - Remove linhas com CPF ou telefone ausentes.
    """
    required_columns = required_columns or ["cpf", "celular_disparo", "nome_completo"]

    # Checa colunas obrigatórias
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Remove linhas com CPF nulo ou telefone vazio
    df = df.dropna(subset=["cpf", "celular_disparo"])
    df = df[df["cpf"].astype(str).str.strip() != ""]
    df = df[df["celular_disparo"].astype(str).str.strip() != ""]

    # Remove duplicatas de telefone
    df = df.drop_duplicates(subset=["celular_disparo"])

    return df.reset_index(drop=True)
