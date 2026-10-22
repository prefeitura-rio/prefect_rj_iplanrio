"""Flow for rj_sme__brutos_gestao_escolar."""

from utils.flows import dump_database

rj_sme__brutos_gestao_escolar = dump_database.with_options(name="rj_sme__brutos_gestao_escolar")
