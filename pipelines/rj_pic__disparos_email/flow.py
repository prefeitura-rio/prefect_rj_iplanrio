# -*- coding: utf-8 -*-
"""
Flow para envio de e-mails em massa com templates HTML.
"""

from typing import Dict, List, Optional
from google.cloud import bigquery
from prefect import flow, task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
import logging
import json
import ast

from pipelines.rj_pic__disparos_email.template_engine import TemplateEngine
from pipelines.rj_pic__disparos_email.email_sender import EmailSender
from pipelines.rj_pic__disparos_email.config import (
    BIGQUERY_PROJECT_ID,
    BIGQUERY_DATASET_ID,
    BIGQUERY_TABLE_ID,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@task
def read_bigquery_task(
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None
    ) -> List[Dict[str, str]]:
    """
    Lê uma tabela do BigQuery e retorna uma lista de dicionários.

    Args:
        project_id: ID do projeto do BigQuery (opcional, usa config se não fornecido)
        dataset_id: ID do dataset do BigQuery (opcional, usa config se não fornecido)
        table_id: ID da tabela do BigQuery (opcional, usa config se não fornecido)
        query: Query SQL customizada (opcional, se fornecida, ignora project_id/dataset_id/table_id)

    Returns:
        Lista de dicionários, onde cada dicionário representa uma linha da tabela
    """

    if not project_id or not dataset_id or not table_id:
        raise ValueError(
            "É necessário fornecer project_id, dataset_id e table_id, "
            "ou definir BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID e BIGQUERY_TABLE_ID no .env, "
            "ou fornecer uma query SQL customizada."
        )
    # Constrói query padrão para ler toda a tabela
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

    try:
        # Inicializa cliente do BigQuery
        client = bigquery.Client(project=project_id)

        # Executa query
        logging.info(f"Executando query no BigQuery: {query[:100]}...")
        query_job = client.query(query)
        results = query_job.result()

        # Converte resultados para lista de dicionários
        rows = []
        for row in results:
            # Converte valores para string para manter compatibilidade
            row_dict = {}
            for key, value in row.items():
                # Converte None para string vazia e outros valores para string
                row_dict[key] = str(value) if value is not None else ""
            rows.append(row_dict)

        logging.info(f"✅ {len(rows)} registros encontrados no BigQuery")
        return rows

    except Exception as e:
        logging.error(f"Erro ao ler dados do BigQuery: {e}")
        raise


@task
def process_email_task(
    row: Dict[str, str],
    template_path: str,
    email_subject: str,
    idx: int,
    total: int
) -> bool:
    """
    Processa e envia um e-mail individual.

    Args:
        row: Dicionário com dados do destinatário
        template_path: Caminho para o template HTML
        email_subject: Assunto do e-mail
        idx: Índice atual (para logs)
        total: Total de registros (para logs)

    Returns:
        True se enviado com sucesso, False caso contrário
    """
    email = row.get('recipiente_email', '').strip()
    nome = row.get('recipiente_nome', '').strip() or email.split('@')[0] if email else ''

    if not email:
        logging.warning(f"Linha {idx}: E-mail vazio, pulando...")
        return False

    print(f"\n[{idx}/{total}] Processando: {nome} ({email})")

    try:
        # Parseia o campo 'dados' que pode vir como string JSON ou representação Python
        dados_str = row.get('dados', '[]')
        alunos = []

        if dados_str:
            try:
                # Tenta primeiro como JSON válido (aspas duplas)
                alunos = json.loads(dados_str)
            except json.JSONDecodeError:
                try:
                    # Se falhar, tenta como representação Python (aspas simples)
                    alunos = ast.literal_eval(dados_str)
                except (ValueError, SyntaxError) as e:
                    logging.warning(f"Erro ao parsear dados na linha {idx}: {e}")
                    logging.warning(f"Conteúdo: {dados_str[:200]}...")
                    alunos = []

        # Garante que alunos é uma lista
        if not isinstance(alunos, list):
            logging.warning(f"Dados parseados não são uma lista na linha {idx}, convertendo...")
            alunos = [alunos] if alunos else []

        # Prepara variáveis para o template
        template_vars = {
            'recipiente_nome': nome,
            'recipiente_email': email,
            'alunos': alunos,
            'data_atualizacao': row.get('data_atualizacao', ''),
            'total_alunos': len(alunos)
        }

        # Cria instância do TemplateEngine e renderiza template
        template_engine = TemplateEngine(template_path)
        html_body = template_engine.render(**template_vars)

        # Cria instância do EmailSender e envia e-mail
        email_sender = EmailSender()
        success = email_sender.send_email(
            to_email=email,
            subject=email_subject,
            html_body=html_body,
            recipient_name=nome
        )

        if success:
            print(f"  ✅ Enviado com sucesso")
            return True
        else:
            print(f"  ❌ Falha no envio")
            return False

    except Exception as e:
        logging.error(f"Erro ao processar linha {idx} ({email}): {e}")
        print(f"  ❌ Erro: {e}")
        return False


@flow(log_prints=True)
def rj_pic__disparos_email(
    email_subject: Optional[str] = "E-mail enviado automaticamente",
):
    """
    Flow para envio de e-mails em massa com templates HTML.

    Este flow lê dados do BigQuery, processa templates HTML e envia e-mails
    personalizados para cada destinatário.

    Args:
        email_subject: Assunto do e-mail
    """
    # Injetar credenciais do BD
    inject_bd_credentials_task(environment="prod")

    try:
        # Lê dados do BigQuery
        print(f"📖 Lendo dados do BigQuery: {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}")
        rows = read_bigquery_task(
            project_id=BIGQUERY_PROJECT_ID,
            dataset_id=BIGQUERY_DATASET_ID,
            table_id=BIGQUERY_TABLE_ID
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
                total=len(rows)
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
        print(f"\n📋 Logs salvos em:")
        print(f"  - erros.log (falhas)")
        print(f"  - sucesso.log (sucessos)")

    except FileNotFoundError as e:
        logging.error(f"Arquivo não encontrado: {e}")
        raise
    except ValueError as e:
        logging.error(f"Erro de validação: {e}")
        raise
    except Exception as e:
        logging.error(f"Erro inesperado: {e}", exc_info=True)
        raise