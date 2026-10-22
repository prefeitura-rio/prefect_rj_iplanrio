"""Flow for rj_smfp__porte_empresa."""

from utils.flows import dump_database

rj_smfp__porte_empresa = dump_database.with_options(name="rj_smfp__porte_empresa")
