# -*- coding: utf-8 -*-
import json

import pandas as pd
from prefect import task

from pipelines.rj_smas__disparo_cadunico.tasks import remove_duplicate_phones


@task
def prepare_destinations(df: pd.DataFrame) -> list[dict]:
    # Deserialize JSON from destination_data column
    destinations = [json.loads(j) for j in df["destination_data"].tolist()]

    # Remove duplicates
    destinations = remove_duplicate_phones(destinations)

    return destinations
