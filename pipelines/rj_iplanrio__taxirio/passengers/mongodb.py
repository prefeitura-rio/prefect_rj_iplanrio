# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any

from pyarrow import string
from pymongoarrow.api import Schema


def generate_pipeline(start: datetime, end: datetime) -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "createdAt": {
                    "$gte": start,
                    "$lt": end,
                },
            },
        },
        {
            "$project": {
                "id": {"$toString": "$_id"},
                "user": {"$toString": "$user"},
                "createdAt": {"$dateToString": {"date": "$createdAt"}},
                "ano_particao": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
                "mes_particao": {"$dateToString": {"format": "%m", "date": "$createdAt"}},
                "dia_particao": {"$dateToString": {"format": "%d", "date": "$createdAt"}},
                "login": 1,
                "password": 1,
                "salt": 1,
                "isAbleToUsePaymentInApp": {"$toString": "$isAbleToUsePaymentInApp"},
                "infoPhone_updatedAt": {"$dateToString": {"date": "$infoPhone.updatedAt"}},
                "infoPhone_id": {"$toString": "$infoPhone._id"},
                "infoPhone_appVersion": "$infoPhone.appVersion",
                "infoPhone_phoneModel": "$infoPhone.phoneModel",
                "infoPhone_phoneManufacturer": "$infoPhone.phoneManufacturer",
                "infoPhone_osVersion": "$infoPhone.osVersion",
                "infoPhone_osName": "$infoPhone.osName",
                "tokenInfo_httpSalt": "$tokenInfo.httpSalt",
                "tokenInfo_wssSalt": "$tokenInfo.wssSalt",
                "tokenInfo_pushToken": "$tokenInfo.pushToken",
            },
        },
        {
            "$unset": "_id",
        },
    ]


schema = Schema(
    {
        "id": string(),
        "user": string(),
        "createdAt": string(),
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "login": string(),
        "password": string(),
        "salt": string(),
        "isAbleToUsePaymentInApp": string(),
        "infoPhone_updatedAt": string(),
        "infoPhone_id": string(),
        "infoPhone_appVersion": string(),
        "infoPhone_phoneModel": string(),
        "infoPhone_phoneManufacturer": string(),
        "infoPhone_osVersion": string(),
        "infoPhone_osName": string(),
        "tokenInfo_httpSalt": string(),
        "tokenInfo_wssSalt": string(),
        "tokenInfo_pushToken": string(),
    },
)
