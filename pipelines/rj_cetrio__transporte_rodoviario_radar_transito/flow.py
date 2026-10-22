"""Flow for rj_cetrio__transporte_rodoviario_radar_transito."""

from utils.flows import dump_database

rj_cetrio__transporte_rodoviario_radar_transito = dump_database.with_options(
    name="rj_cetrio__transporte_rodoviario_radar_transito"
)
