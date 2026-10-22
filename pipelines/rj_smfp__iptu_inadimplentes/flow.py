"""Flow for rj_smfp__iptu_inadimplentes."""

from utils.flows import dump_database

rj_smfp__iptu_inadimplentes = dump_database.with_options(name="rj_smfp__iptu_inadimplentes")
