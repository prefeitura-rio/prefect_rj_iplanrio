"""Flow for rj_smfp__adm_instrumentos_firmados."""

from utils.flows import dump_database

rj_smfp__adm_instrumentos_firmados = dump_database.with_options(name="rj_smfp__adm_instrumentos_firmados")
