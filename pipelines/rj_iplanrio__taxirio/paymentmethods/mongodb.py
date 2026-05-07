# -*- coding: utf-8 -*-
from pyarrow import string
from pymongoarrow.api import Schema

pipeline = [
    {
        "$project": {
            "id": {"$toString": "$_id"},
            "pindex": {"$toString": "$pindex"},
            "name": 1,
            "type": 1,
        },
    },
    {
        "$unset": "_id",
    },
]

schema = Schema(
    {
        "id": string(),
        "pindex": string(),
        "name": string(),
        "type": string(),
    },
)
