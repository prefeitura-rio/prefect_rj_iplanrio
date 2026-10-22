"""Flow for rj_smfp__nota_carioca_oracle_to_bq."""

from utils.flows import dump_database

rj_smfp__nota_carioca_oracle_to_bq = dump_database.with_options(name="rj_smfp__nota_carioca_oracle_to_bq")
