"""Flow for rj_cvl__osinfo."""

from utils.flows import dump_database

rj_cvl__osinfo = dump_database.with_options(name="rj_cvl__osinfo")
