# -*- coding: utf-8 -*-
import warnings
from pathlib import Path
from ruamel.yaml import YAML
import os
from copy import deepcopy

# Ignora warnings do Pydantic/Python 3.14
warnings.filterwarnings("ignore")
print("Current Working Directory:", os.getcwd())
# Caminhos relativos ao WORKDIR do Dockerfile (/opt/prefect/prefect_rj_iplanrio)
# base_path = Path("prefect_base.yaml")
# base_path = Path("pipelines/rj_crm__disparo_template/prefect.yaml")
# sched_path = Path("pipelines/rj_crm__disparo_template/scheduler.yaml")
# out_path = Path("pipelines/rj_crm__disparo_template/prefect.yaml")

base_path = Path("prefect.yaml")
sched_path = Path("scheduler.yaml")
out_path = Path("prefect.yaml")

# Inicializa ruamel.yaml
yaml = YAML()
yaml.preserve_quotes = True


# Carrega arquivos
with base_path.open() as f:
    base = yaml.load(f)
with sched_path.open() as f:
    sched = yaml.load(f)

# Aplica scheduler a todos os deployments (sem duplicar)
for dep in base.get("deployments", []):
    # Se já existir, substitui; se não, adiciona
    dep["schedules"] = deepcopy(sched.get("schedules", []))

# Escreve o YAML final limpo
with out_path.open("w") as f:
    yaml.dump(base, f)

print(f"Arquivo atualizado: {out_path}")