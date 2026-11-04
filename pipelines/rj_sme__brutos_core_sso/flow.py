"""Flow for rj_sme__brutos_core_sso."""

from utils.flows import dump_database

rj_sme__brutos_core_sso = dump_database.with_options(name="rj_sme__brutos_core_sso")
