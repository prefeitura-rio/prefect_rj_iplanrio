# -*- coding: utf-8 -*-
import pandas as pd
import yaml
from core.pipeline import TransformPipeline
from prefect import task


@task
def transform_data(df: pd.DataFrame, config_path: str) -> pd.DataFrame:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    pipeline = TransformPipeline.from_config(config)
    return pipeline.run(df)
