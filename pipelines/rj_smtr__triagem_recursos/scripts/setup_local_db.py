import sys
from pathlib import Path
import psycopg2

PIPELINE_DIR = Path(__file__).resolve().parent.parent

DB_URL = "postgresql://postgres:postgres@localhost:5432/smtr_triagem"
SQL_FILE = PIPELINE_DIR / "queries" / "create_db.sql"


def main():
    print(f"Conectando ao PostgreSQL local em: {DB_URL}")
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    print(f"Executando DDL: {SQL_FILE}")
    if not SQL_FILE.exists():
        raise FileNotFoundError(f"Arquivo DDL nao encontrado em {SQL_FILE}")

    ddl_sql = SQL_FILE.read_text(encoding="utf-8")
    cursor.execute(ddl_sql)
    conn.commit()
    print("✅ Tabelas criadas com sucesso.")
    
    nome_pdf = "CLONE (2).pdf"
    numero_sei = "SEI-0025007/2026/05"

    print(f"Inserindo processo de teste: {nome_pdf}")
    cursor.execute(
        """
        INSERT INTO processos_administrativos (
            numero_processo_sei,
            numero_auto_infracao,
            origem_lote,
            nome_arquivo_origem
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (numero_processo_sei) DO UPDATE SET nome_arquivo_origem = EXCLUDED.nome_arquivo_origem
        RETURNING processo_id;
    """,
        (numero_sei, "V01234567", "MENSAL", nome_pdf),
    )

    processo_id = cursor.fetchone()[0]

    # 3. Insere na fila de trabalho com status PENDENTE_IA
    cursor.execute(
        """
        INSERT INTO fila_trabalho (
            processo_id,
            perfil_requerido,
            status,
            prioridade_ordenacao
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """,
        (processo_id, "JARISTA", "PENDENTE_IA", 0.90),
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(
        "Processo inserido na fila com status 'PENDENTE_IA'. Banco pronto para teste."
    )


if __name__ == "__main__":
    main()