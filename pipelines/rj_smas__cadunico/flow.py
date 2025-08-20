# -*- coding: utf-8 -*-
from prefect import flow, unmapped

from pipelines.rj_smas__cadunico.tasks import (
    append_data_to_storage,
    create_table_if_not_exists,
    get_existing_partitions,
    get_files_to_ingest,
    ingest_file,
    need_to_ingest,
)


@flow(log_prints=True)
def rj_smas__cadunico(  # noqa
    prefix_raw_area="raw/protecao_social_cadunico/registro_familia",
    prefix_staging_area="staging/protecao_social_cadunico/registro_familia",
    ingested_files_output="/tmp/ingested_files/",
    dataset_id="brutos_cadunico",
    table_id="registro_familia",
    dump_mode="append",
):
    existing_partitions = get_existing_partitions(prefix=prefix_staging_area, bucket_name="rj-iplanrio")
    files_to_ingest = get_files_to_ingest(prefix=prefix_raw_area, partitions=existing_partitions, bucket_name="rj-smas")
    need_to_ingest_bool = need_to_ingest(files_to_ingest=files_to_ingest)

    if need_to_ingest_bool:
        ingested_files = ingest_file.map(
            blob=files_to_ingest,
            output_directory=unmapped(ingested_files_output),
        )
        create_table = create_table_if_not_exists(
            dataset_id=dataset_id,
            table_id=table_id,
            wait_for=[ingested_files],
        )
        _ = append_data_to_storage(
            data_path=ingested_files_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            wait_for=[create_table],
        )
