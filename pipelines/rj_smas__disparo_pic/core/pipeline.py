# -*- coding: utf-8 -*-
import pandas as pd
from core.registry import TransformerRegistry


class TransformPipeline:
    def __init__(self):
        self.steps = []

    def add(self, name: str, **kwargs):
        transformer = TransformerRegistry.create(name, **kwargs)
        self.steps.append(transformer)
        return self

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        for t in self.steps:
            df = t.transform(df)
        return df

    @classmethod
    def from_config(cls, config: dict):
        pipeline = cls()
        for step in config["transformations"]:
            pipeline.add(step["method"], **step["args"])
        return pipeline
