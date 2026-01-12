# -*- coding: utf-8 -*-
# python3 -m rj_crm__disparo_template/build_prefect_yaml --env prod
import warnings
from pathlib import Path
from ruamel.yaml import YAML
import os
from copy import deepcopy
import argparse

# Ignora warnings do Pydantic/Python 3.14
warnings.filterwarnings("ignore")
print("Current Working Directory:", os.getcwd())
# Caminhos relativos ao WORKDIR do Dockerfile (/opt/prefect/prefect_rj_iplanrio)
# base_path = Path("prefect_base.yaml")
# base_path = Path("pipelines/rj_crm__disparo_template/prefect.yaml")
# sched_path = Path("pipelines/rj_crm__disparo_template/scheduler.yaml")
# out_path = Path("pipelines/rj_crm__disparo_template/prefect.yaml")


# --- Argument Parser ---
parser = argparse.ArgumentParser(description="Build prefect.yaml with specific schedules.")
parser.add_argument(
    "--env",
    choices=["staging", "prod", "both"],
    default="both",
    help="Specify the environment to add schedules to: 'staging', 'prod', or 'both'. Defaults to 'both'.",
)
args = parser.parse_args()
# ---

# Caminhos relativos
base_path = Path("prefect.yaml")
sched_path = Path("scheduler.yaml")
out_path = Path("prefect.yaml")

# Inicializa ruamel.yaml
yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)


# Carrega arquivos
with base_path.open() as f:
    base = yaml.load(f)
with sched_path.open() as f:
    sched = yaml.load(f)

# Aplica scheduler e parâmetros aos deployments com base no argumento --env
for dep in base.get("deployments", []):
    dep_name = dep.get("name", "")

    # Lógica para staging
    if "staging" in dep_name:
        if args.env in ["staging", "both"]:
            # Copia schedules e adiciona parâmetros a cada um
            staging_schedules = deepcopy(sched.get("schedules", []))
            for schedule in staging_schedules:
                schedule["active"] = False
                if "parameters" not in schedule:
                    schedule["parameters"] = {}  # ruamel.yaml will convert this to CommentedMap
                schedule["parameters"].insert(0, "flow_environment", "staging")
            dep["schedules"] = staging_schedules
            print(f"Schedules with 'flow_environment: staging' applied to '{dep_name}'.")

        elif "schedules" in dep:
            # Remove schedules se não for o ambiente alvo
            del dep["schedules"]
            print(f"Schedules removed from '{dep_name}'.")

    # Lógica para prod
    elif "prod" in dep_name:
        if args.env in ["prod", "both"]:
            # Copia schedules e adiciona parâmetros a cada um
            prod_schedules = deepcopy(sched.get("schedules", []))
            for schedule in prod_schedules:
                if "parameters" not in schedule:
                    schedule["parameters"] = {}
                schedule["parameters"].insert(0, "flow_environment", "production")
            dep["schedules"] = prod_schedules
            print(f"Schedules with 'flow_environment: production' applied to '{dep_name}'.")

        elif "schedules" in dep:
            # Remove schedules se não for o ambiente alvo
            del dep["schedules"]
            print(f"Schedules removed from '{dep_name}'.")

    # Remove o 'parameters' de nível superior se estiver vazio, para limpeza
    if "parameters" in dep and not dep["parameters"]:
        del dep["parameters"]


# Escreve o YAML final limpo
with out_path.open("w") as f:
    yaml.dump(base, f)

print(f"Arquivo atualizado: {out_path} para o ambiente: {args.env}")