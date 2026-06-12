# -*- coding: utf-8 -*-
from pyarrow import string
from pymongoarrow.api import Schema

pipeline = [
    {
        "$project": {
            "id": {"$toString": "$_id"},
            "user": {"$toString": "$user"},
            "taxiDriverId": 1,
            "cars": {
                "$map": {
                    "input": "$cars",
                    "as": "car",
                    "in": {"$toString": "$$car"},
                },
            },
            "average": {"$toString": "$average"},
            "associatedCar": {"$toString": "$associatedCar"},
            "createdAt": {"$dateToString": {"date": "$createdAt"}},
            "ano_particao": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
            "mes_particao": {"$dateToString": {"format": "%m", "date": "$createdAt"}},
            "dia_particao": {"$dateToString": {"format": "%d", "date": "$createdAt"}},
            "status": 1,
            "associatedDiscount": {"$toString": "$associatedDiscount"},
            "associatedPaymentsMethods": {
                "$map": {
                    "input": "$associatedPaymentsMethods",
                    "as": "paymentMethod",
                    "in": {"$toString": "$$paymentMethod"},
                },
            },
            "login": 1,
            "password": 1,
            "salt": 1,
            "isAbleToReceivePaymentInApp": {"$toString": "$isAbleToReceivePaymentInApp"},
            "isAbleToReceivePaymentInCityHall": {"$toString": "$isAbleToReceivePaymentInCityHall"},
            "ratingsReceived": {"$toString": "$ratingsReceived"},
            "busy": {"$toString": "$busy"},
            "associatedRace_originAtAccepted_position_lng": {
                "$toString": "$associatedRace.originAtAccepted.position.lng",
            },
            "associatedRace_originAtAccepted_position_lat": {
                "$toString": "$associatedRace.originAtAccepted.position.lat",
            },
            "associatedRace_race": {"$toString": "$associatedRace.race"},
            "lastAverage": {"$toString": "$lastAverage"},
            "expiredBlockByRankingDate": {"$toString": "$expiredBlockByRankingDate"},
            "blockedRace": {"$toString": "$blockedRace"},
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
            "city": {"$toString": "$city"},
            "serviceRecordRate": {"$toString": "$serviceRecordRate"},
            "nota": {"$toString": "$nota"},
            "averageTT": {"$toString": "$averageTT"},
        },
    },
    {
        "$unset": "_id",
    },
    {
        "$addFields": {
            "cars": {
                "$function": {
                    "lang": "js",
                    "args": ["$cars"],
                    "body": "function(x) { return JSON.stringify(x); }",
                },
            },
            "associatedPaymentsMethods": {
                "$function": {
                    "lang": "js",
                    "args": ["$associatedPaymentsMethods"],
                    "body": "function(x) { return JSON.stringify(x); }",
                },
            },
        },
    },
]


schema = Schema(
    {
        "id": string(),
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "createdAt": string(),
        "user": string(),
        "taxiDriverId": string(),
        "cars": string(),
        "average": string(),
        "associatedCar": string(),
        "status": string(),
        "associatedDiscount": string(),
        "associatedPaymentsMethods": string(),
        "login": string(),
        "password": string(),
        "salt": string(),
        "isAbleToReceivePaymentInApp": string(),
        "isAbleToReceivePaymentInCityHall": string(),
        "ratingsReceived": string(),
        "busy": string(),
        "lastAverage": string(),
        "expiredBlockByRankingDate": string(),
        "blockedRace": string(),
        "city": string(),
        "serviceRecordRate": string(),
        "nota": string(),
        "averageTT": string(),
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
        "associatedRace_originAtAccepted_position_lng": string(),
        "associatedRace_originAtAccepted_position_lat": string(),
        "associatedRace_race": string(),
    },
)
