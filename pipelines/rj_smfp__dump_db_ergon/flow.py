"""Flow for rj_smfp__dump_db_ergon."""

from utils.flows import dump_database

rj_smfp__dump_db_ergon = dump_database.with_options(name="rj_smfp__dump_db_ergon")
