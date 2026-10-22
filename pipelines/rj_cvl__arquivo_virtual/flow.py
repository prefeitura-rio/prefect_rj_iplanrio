"""Flow for rj_cvl__arquivo_virtual."""

from utils.flows import dump_database

rj_cvl__arquivo_virtual = dump_database.with_options(name="rj_cvl__arquivo_virtual")
