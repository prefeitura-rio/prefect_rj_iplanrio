# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any

from pyarrow import string
from pymongoarrow.api import Schema


def generate_pipeline(start: datetime, end: datetime) -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "dateTime": {
                    "$gte": start,
                    "$lt": end,
                },
            },
        },
        {
            "$project": {
                "id": {"$toString": "$_id"},
                "driver": {"$toString": "$driver"},
                "associatedDiscount": {"$toString": "$associatedDiscount"},
                "ano_particao": {"$dateToString": {"format": "%Y", "date": "$dateTime"}},
                "mes_particao": {"$dateToString": {"format": "%m", "date": "$dateTime"}},
                "dia_particao": {"$dateToString": {"format": "%d", "date": "$dateTime"}},
                "dateTime": {"$toString": "$dateTime"},
            },
        },
        {
            "$unset": "_id",
        },
    ]


schema = Schema(
    {
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "id": string(),
        "driver": string(),
        "associatedDiscount": string(),
        "dateTime": string(),
    },
)
