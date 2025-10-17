import pandas as pd
import json
from prefect import task
from pipelines.rj_smas__disparo_cadunico.tasks import remove_duplicate_phones


@task
def prepare_destinations(df: pd.DataFrame) -> list[dict]:
    df = remove_duplicate_phones(df)
    return [json.loads(j) for j in df["destination_data"].tolist()]
