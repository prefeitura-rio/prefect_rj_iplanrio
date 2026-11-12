# -*- coding: utf-8 -*-
from pyarrow import string
from pymongoarrow.api import Schema

pipeline = [
    {
        "$project": {
            "id": {"$toString": "$_id"},
            "name": 1,
            "stateAbbreviation": 1,
            "isCalulatedInApp": {"$toString": "$isCalulatedInApp"},
            "isAbleToUsePaymentInApp": {"$toString": "$isAbleToUsePaymentInApp"},
            "loginLabel": 1,
            "serviceStations": {
                "$function": {
                    "lang": "js",
                    "args": ["$serviceStations"],
                    "body": "function(x) { return JSON.stringify(x); }",
                },
            },
        },
    },
    {
        "$unset": "_id",
    },
]

schema = Schema(
    {
        "id": string(),
        "name": string(),
        "stateAbbreviation": string(),
        "isCalulatedInApp": string(),
        "isAbleToUsePaymentInApp": string(),
        "loginLabel": string(),
        "serviceStations": string(),
    },
)
