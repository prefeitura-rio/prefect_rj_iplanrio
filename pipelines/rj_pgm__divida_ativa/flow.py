"""Flow for rj_pgm__divida_ativa."""

from utils.flows import dump_database

rj_pgm__divida_ativa = dump_database.with_options(name="rj_pgm__divida_ativa")
