"""Script de inicialização e migração do esquema do banco de dados local.

Executa o DDL consolidado em ``queries/create_db.sql`` e insere um registro de teste.
"""

import logging
from pathlib import Path
import psycopg2

from pipelines.rj_smtr__triagem_recursos.constants import (
    PIPELINE_DIR,
    StatusProcessamento,
    resolver_banco_url,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("setup_local_db")

SQL_FILE = PIPELINE_DIR / "queries" / "create_db.sql"


def main() -> None:
    """Executa a criação de tabelas e insere processo piloto para testes locais.

    :raises FileNotFoundError: Caso o arquivo SQL DDL não seja encontrado.
    :raises psycopg2.OperationalError: Caso a conexão com o PostgreSQL falhe.
    """
    db_url = resolver_banco_url(is_local=True)
    logger.info("Conectando ao PostgreSQL local em: %s", db_url)

    if not SQL_FILE.exists():
        raise FileNotFoundError(f"Arquivo DDL não encontrado em {SQL_FILE}")

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    try:
        logger.info("Executando DDL consolidado: %s", SQL_FILE)
        ddl_sql = SQL_FILE.read_text(encoding="utf-8")
        cursor.execute(ddl_sql)
        conn.commit()
        logger.info("Tabelas, índices e views criados com sucesso.")

        nome_pdf = "CLONE (2).pdf"
        numero_sei = "SEI-0025007/2026/05"

        logger.info("Inserindo processo de teste piloto: %s (%s)", numero_sei, nome_pdf)
        cursor.execute(
            """
            INSERT INTO processos_administrativos (
                numero_processo_sei,
                numero_auto_infracao,
                origem_lote,
                nome_arquivo_origem,
                codigo_jari,
                status_distribuicao
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (numero_processo_sei) DO UPDATE SET 
                nome_arquivo_origem = EXCLUDED.nome_arquivo_origem
            RETURNING processo_id;
            """,
            (numero_sei, "V01234567", "MENSAL", nome_pdf, "1_JARI", "PENDENTE"),
        )
        processo_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO fila_trabalho (
                processo_id,
                status
            ) VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (processo_id, StatusProcessamento.PENDENTE_IA.value),
        )

        conn.commit()
        logger.info(
            "Processo piloto %s inserido na fila com status '%s'. Banco pronto para operação.",
            processo_id,
            StatusProcessamento.PENDENTE_IA.value,
        )
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
