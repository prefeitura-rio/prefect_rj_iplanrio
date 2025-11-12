import yaml
from pathlib import Path
import os

print(os.listdir())
print(os.getcwd())

# Caminhos relativos ao WORKDIR do Dockerfile (/opt/prefect/prefect_rj_iplanrio)
# base_path = Path("prefect_base.yaml")
base_path = Path("pipelines/rj_smas__disparo_pic/prefect.yaml")
sched_path = Path("pipelines/rj_smas__disparo_pic/scheduler.yaml")
out_path = Path("pipelines/rj_smas__disparo_pic/prefect.yaml")

# Carrega arquivos
with base_path.open() as f:
    base = yaml.safe_load(f)
with sched_path.open() as f:
    sched = yaml.safe_load(f)

# Aplica scheduler a todos os deployments (sem duplicar)
for dep in base.get("deployments", []):
    # Se já existir, substitui; se não, adiciona
    dep["schedules"] = sched.get("schedules", [])

# Garante que não haja anchors (&id001)
# Isso evita que o PyYAML gere aliases automáticos
yaml.SafeDumper.ignore_aliases = lambda *args: True

# Escreve o YAML final limpo
with out_path.open("w") as f:
    yaml.safe_dump(base, f, sort_keys=False, allow_unicode=True)

print(f"Arquivo atualizado: {out_path}")