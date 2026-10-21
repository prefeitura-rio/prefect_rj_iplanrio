"""Enriquece configuracoes_calibracao com os perfis médios das classes do Goldenset."""

import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

PIPELINE_DIR = Path(__file__).resolve().parent.parent

try:
  from dotenv import find_dotenv, load_dotenv

  load_dotenv(find_dotenv(usecwd=True))
except ImportError:
  pass

from pipelines.rj_smtr__triagem_recursos.constants import resolver_banco_url


def main():
  db_url = resolver_banco_url(is_local=True)
  conn = psycopg2.connect(db_url)

  with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # 1. Calcula os perfis médios reais por hipótese no Goldenset
    cur.execute("""
            SELECT 
                ouro_hipotese,
                AVG(feat_sim_centroid_pred) as media_sim,
                AVG(feat_mahalanobis_dist) as media_maha
            FROM goldenset_processos
            WHERE feat_sim_centroid_pred IS NOT NULL
            GROUP BY ouro_hipotese;
        """)
    perfis_classes = {
        r["ouro_hipotese"]: {
            "media_sim": round(float(r["media_sim"]), 4),
            "media_maha": round(float(r["media_maha"]), 4),
        }
        for r in cur.fetchall()
    }

    # 2. Recupera e atualiza a configuracao ativa
    cur.execute(
        "SELECT calibracao_id, estatisticas_classes FROM"
        " configuracoes_calibracao WHERE ativo = TRUE;"
    )
    cfg = cur.fetchone()
    stats = cfg["estatisticas_classes"] if cfg else {}
    if isinstance(stats, str):
      stats = json.loads(stats)

    stats["perfis_classes_goldenset"] = perfis_classes

    cur.execute(
        """
            UPDATE configuracoes_calibracao
            SET estatisticas_classes = %s
            WHERE ativo = TRUE;
        """,
        (json.dumps(stats),),
    )

  conn.commit()
  conn.close()
  print("Parâmetros de classes atualizados com base nos 730 casos:")
  print(json.dumps(perfis_classes, indent=2))


if __name__ == "__main__":
  main()