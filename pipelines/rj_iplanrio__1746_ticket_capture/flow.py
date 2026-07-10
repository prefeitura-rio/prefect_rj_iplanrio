from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger
from iplanrio.pipelines_templates.dump_db.tasks import get_database_username_and_password_from_secret_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
import pymssql
import pandas as pd
import traceback

@task(retries=0)
def fetch_data_from_db(query: str, config: dict):
    logger = get_run_logger()
    logger.info(f"Conectando ao banco na query: {query}")
    try:
        conn = pymssql.connect(**config)
        df = pd.read_sql(query, conn)
        logger.info(f"Query retornou {len(df)} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro ao executar a query:\n{traceback.format_exc()}")
        raise e
    finally:
        if 'conn' in locals() and conn:
            conn.close()

@flow(name="rj_iplanrio__1746_ticket_capture", log_prints=True)
def rj_iplanrio__1746_ticket_capture(
    query: str = "SELECT TOP 100 name FROM sys.tables;",
    db_host: str = "10.6.99.27",
    db_port: str = "1433",
    db_database: str = "pcrj",
    infisical_secret_path: str = "/db-1746-prod-server"
):
    """
    Fluxo para rodar queries exploratórias no banco do 1746 via Prefect,
    utilizando secrets da Infisical para nao expor credenciais.
    """
    logger = get_run_logger()
    
    # Injetar secrets ambientais
    inject_bd_credentials_task(environment="prod")
    
    # Buscar secrets do Infisical para nao expor a credencial do banco
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    
    # Montar configuração do banco
    config = {
        'server': db_host,
        'port': int(db_port),
        'user': secrets["DB_USERNAME"],
        'password': secrets["DB_PASSWORD"],
        'database': db_database,
        'timeout': 60
    }
    
    try:
        df = fetch_data_from_db(query, config)
        
        # Cria um artefato markdown no Prefect para facilitar a visualização dos resultados
        markdown_table = df.head(100).to_markdown(index=False)
        artifact_content = (
            f"### Resultados da Query Exploratória\n"
            f"**Query Executada:**\n```sql\n{query}\n```\n\n"
            f"**Total de Linhas (no DataFrame):** {len(df)}\n"
            f"**Amostra (Max 100):**\n\n{markdown_table}"
        )
        create_markdown_artifact(
            key="exploracao-db-1746",
            markdown=artifact_content,
            description="Amostra dos resultados exploratórios do banco 1746"
        )
        logger.info("Resultados salvos como um Artifact no Prefect. Vá na aba 'Artifacts' para ver a tabela completa!")
        
    except Exception as e:
        logger.error(f"Falha na exploração: {e}")

if __name__ == "__main__":
    # Teste isolado nao recomendavel por conta de conexões dependentes do infisical.
    pass
