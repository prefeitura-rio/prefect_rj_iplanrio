"""
Flow para envio de e-mails em massa com templates HTML..
"""
import logging

from prefect import flow

from pipelines.rj_pic__disparos_email.settings import Settings
from pipelines.rj_pic__disparos_email.tasks import (
    process_email_task,
    read_bigquery_task,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@flow(log_prints=True)  # Decorador comentado temporariamente
def rj_pic__disparos_email(
    email_subject: str = "E-mail enviado automaticamente",
):
    """
    Flow para envio de e-mails em massa com templates HTML via Data Relay API.

    Este flow lê dados do BigQuery, processa templates HTML e envia e-mails
    personalizados para cada destinatário usando a API do Data Relay.

    Args:
        email_subject: Assunto do e-mail
    """
    # Injetar credenciais do BD

    settings = Settings()

    try:
        # Lê dados do BigQuery
        print(
            f"📖 Lendo dados do BigQuery: {settings.BIGQUERY_PROJECT_ID}.{settings.BIGQUERY_DATASET_ID}.{settings.BIGQUERY_TABLE_ID}"
        )
        rows = read_bigquery_task(
            project_id=settings.BIGQUERY_PROJECT_ID,
            dataset_id=settings.BIGQUERY_DATASET_ID,
            table_id=settings.BIGQUERY_TABLE_ID,
        )
        print(f"✅ {len(rows)} registros encontrados no BigQuery")

        # Processa cada linha
        print(f"\n🚀 Iniciando envio de e-mails...\n")
        print("=" * 60)

        success_count = 0
        error_count = 0

        for idx, row in enumerate(rows, 1):
            success = process_email_task(
                row=row,
                template_path="email_templates.html",
                email_subject=email_subject,
                idx=idx,
                total=len(rows),
            )

            if success:
                success_count += 1
            else:
                error_count += 1

        # Resumo final
        print("\n" + "=" * 60)
        print(f"\n📊 Resumo do envio:")
        print(f"  ✅ Sucessos: {success_count}")
        print(f"  ❌ Falhas: {error_count}")
        print(f"  📝 Total: {len(rows)}")

    except FileNotFoundError as e:
        logging.error(f"Arquivo não encontrado: {e}")
        raise
    except ValueError as e:
        logging.error(f"Erro de validação: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado: {e}", exc_info=True)
        raise
