# -*- coding: utf-8 -*-
import json

import pandas as pd
from core.base import TransformStrategy
from core.registry import TransformerRegistry


@TransformerRegistry.register("Rename")
class RenameTransform(TransformStrategy):
    def __init__(self, **rename_map):
        self.rename_map = rename_map

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_map)


@TransformerRegistry.register("TitleCase")
class TitleCaseTransform(TransformStrategy):
    def __init__(self, column: str):
        self.column = column

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column] = df[self.column].astype("string").str.strip().str.title()
        return df


@TransformerRegistry.register("NormalizeCPF")
class NormalizeCPFTransform(TransformStrategy):
    def __init__(self, column: str):
        self.column = column

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.column] = df[self.column].astype("string").str.replace(r"\D", "", regex=True)
        return df


@TransformerRegistry.register("BuildDestinationJSON")
class BuildDestinationJSON(TransformStrategy):
    """
    Monta a coluna destination_data com base em um template declarativo.
    Sempre injeta:
        - vars.CC_WT_CPF_CIDADAO = df['cpf']
        - externalId = df['cpf']
    """

    def __init__(self, template: dict, output_col: str = "destination_data"):
        self.template = template
        self.output_col = output_col

    def _render(self, template_obj, row):
        if isinstance(template_obj, str):
            try:
                return template_obj.format(**row)
            except KeyError as e:
                raise ValueError(f"Missing column for placeholder {e.args[0]}")
        elif isinstance(template_obj, dict):
            return {k: self._render(v, row) for k, v in template_obj.items()}
        elif isinstance(template_obj, list):
            return [self._render(v, row) for v in template_obj]
        else:
            return template_obj

    def _inject_required_fields(self, rendered_json: dict, row: dict) -> dict:
        cpf_value = row.get("cpf") or row.get("CPF")
        if not cpf_value:
            raise ValueError("Column 'cpf' is required to build destination_data")

        rendered_json.setdefault("vars", {})
        rendered_json["vars"]["CC_WT_CPF_CIDADAO"] = cpf_value
        rendered_json["externalId"] = cpf_value
        return rendered_json

    def transform(self, df):
        df = df.copy()
        rendered_rows = []
        for _, row in df.iterrows():
            rendered = self._render(self.template, row.to_dict())
            rendered = self._inject_required_fields(rendered, row.to_dict())
            rendered_rows.append(json.dumps(rendered))
        df[self.output_col] = rendered_rows
        return df
