"""Flow for rj_cgm__osinfo_rh."""

from utils.flows import dump_database

rj_cgm__osinfo_rh = dump_database.with_options(name="rj_cgm__osinfo_rh")
