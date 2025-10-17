import yaml
import pandas as pd
from prefect import task
from core.pipeline import TransformPipeline


@task
def transform_data(df: pd.DataFrame, config_path: str) -> pd.DataFrame:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    pipeline = TransformPipeline.from_config(config)
    return pipeline.run(df)
