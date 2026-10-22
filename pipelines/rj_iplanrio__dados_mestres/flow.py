"""Flow for rj_iplanrio__dados_mestres."""

from utils.flows import dump_arcgis

rj_iplanrio__dados_mestres = dump_arcgis.with_options(name="rj_iplanrio__dados_mestres")
