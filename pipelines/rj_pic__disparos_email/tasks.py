# -*- coding: utf-8 -*-
"""
Task para envio de e-mails em massa com templates HTML.
"""

from typing import Dict, List, Optional
from google.cloud import bigquery
from prefect import task
import json
import ast
import os

from pipelines.rj_pic__disparos_email.engine import TemplateEngine
from pipelines.rj_pic__disparos_email.engine import EmailSender
from pipelines.rj_pic__disparos_email.env import FILTER_EMAILS


@task(log_prints=True)
def read_bigquery_task(
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
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
    query = f"""
        SELECT
            *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """

    if "@" in FILTER_EMAILS:
        lista_emails = [f"'{email.strip()}'" for email in FILTER_EMAILS.split(",")]
        separador = ",\n             "
        filters = separador.join(lista_emails)
        query += f"""    WHERE recipiente_email IN (
             {filters}
         )
        """

    try:
        # Inicializa cliente do BigQuery
        client = bigquery.Client(project=project_id)

        # Executa query
        print(f"Executando query no BigQuery:\n{query}")
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

        print(f"✅ {len(rows)} registros encontrados no BigQuery")
        return rows

    except Exception as e:
        print(f"Erro ao ler dados do BigQuery: {e}")
        raise


@task(log_prints=True)
def process_email_task(
    row: Dict[str, str], template_path: str, email_subject: str, idx: int, total: int
) -> bool:
    """
    Processa e envia um e-mail individual via Data Relay API.

    Args:
        row: Dicionário com dados do destinatário
        template_path: Caminho para o template HTML
        email_subject: Assunto do e-mail
        idx: Índice atual (para logs)
        total: Total de registros (para logs)

    Returns:
        True se enviado com sucesso, False caso contrário
    """
    email = row.get("recipiente_email", "").strip()
    nome = (
        row.get("recipiente_nome", "").strip() or email.split("@")[0] if email else ""
    )

    if not email:
        print(f"Linha {idx}: E-mail vazio, pulando...")
        return False

    print(f"\n[{idx}/{total}] Processando: {nome} ({email})")

    try:
        # Parseia o campo 'dados' que pode vir como string JSON ou representação Python
        dados_str = row.get("dados", "[]")
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
                    print(f"Erro ao parsear dados na linha {idx}: {e}")
                    print(f"Conteúdo: {dados_str[:200]}...")
                    alunos = []

        # Garante que alunos é uma lista
        if not isinstance(alunos, list):
            print(f"Dados parseados não são uma lista na linha {idx}, convertendo...")
            alunos = [alunos] if alunos else []

        # Prepara variáveis para o template
        template_vars = {
            "recipiente_nome": nome,
            "recipiente_email": email,
            "alunos": alunos,
            "data_atualizacao": row.get("data_atualizacao", ""),
            "total_alunos": len(alunos),
        }

        # Cria instância do TemplateEngine e renderiza template

        template_path = os.path.join(
            os.getcwd(), "pipelines/rj_pic__disparos_email/email_template.html"
        )
        template_engine = TemplateEngine(template_path)
        html_body = template_engine.render(**template_vars)

        # Cria instância do EmailSender e envia e-mail
        email_sender = EmailSender()
        success = email_sender.send_email(
            to_email=email,
            subject=email_subject,
            html_body=html_body,
            recipient_name=nome,
        )
        if success:
            print(f"  ✅ Enviado com sucesso")
            return True
        else:
            print(f"  ❌ Falha no envio")
            return False

    except Exception as e:
        print(f"Erro ao processar linha {idx} ({email}): {e}")
        print(f"  ❌ Erro: {e}")
        return False
