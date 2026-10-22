"""Flow for rj_smfp__atividade_economica."""

from utils.flows import dump_database

rj_smfp__atividade_economica = dump_database.with_options(name="rj_smfp__atividade_economica")
