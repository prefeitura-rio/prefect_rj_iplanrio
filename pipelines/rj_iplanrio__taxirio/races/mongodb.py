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
                "car": {"$toString": "$car"},
                "driver": {"$toString": "$driver"},
                "createdAt": {"$dateToString": {"date": "$createdAt"}},
                "finishedAt": {"$dateToString": {"date": "$finishedAt"}},
                "ano_particao": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
                "mes_particao": {"$dateToString": {"format": "%m", "date": "$createdAt"}},
                "dia_particao": {"$dateToString": {"format": "%d", "date": "$createdAt"}},
                "event": {"$toString": "$event"},
                "passenger": {"$toString": "$passenger"},
                "city": {"$toString": "$city"},
                "broadcastQtd": {"$toString": "$broadcastQtd"},
                "rating_score": {"$toString": "$rating.score"},
                "isSuspect": {"$toString": "$isSuspect"},
                "isInvalid": {"$toString": "$isInvalid"},
                "billing_estimatedPrice": {"$toString": "$billing.estimatedPrice"},
                "billing_associatedPaymentMethod": {"$toString": "$billing.associatedPaymentMethod"},
                "billing_associatedTaximeter": {"$toString": "$billing.associatedTaximeter"},
                "billing_associatedMinimumFare": {"$toString": "$billing.associatedMinimumFare"},
                "billing_associatedDiscount": {"$toString": "$billing.associatedDiscount"},
                "billing_associatedCorporative_externalPropertyPassenger": {
                    "$toString": "$billing.associatedCorporative.externalPropertyPassenger",
                },
                "billing_finalPrice_totalToPay": "$billing.finalPrice.totalToPay",
                "billing_finalPrice_totalPriceToll": "$billing.finalPrice.totalPriceToll",
                "billing_finalPrice_totalWithDiscount": "$billing.finalPrice.totalWithDiscount",
                "billing_finalPrice_totalDiscount": "$billing.finalPrice.totalDiscount",
                "billing_finalPrice_totalWithoutDiscount": "$billing.finalPrice.totalWithoutDiscount",
                "billing_finalPrice_totalByTaximeter": "$billing.finalPrice.totalByTaximeter",
                "billing_finalPrice_totalByStoppedTime": "$billing.finalPrice.totalByStoppedTime",
                "billing_finalPrice_totalByKm": "$billing.finalPrice.totalByKm",
                "billing_finalPrice_minPrice": "$billing.finalPrice.minPrice",
                "status": 1,
                "routeOriginDestination_distance_text": "$routeOriginDestination.distance.text",
                "routeOriginDestination_distance_value": {"$toString": "$routeOriginDestination.distance.value"},
                "routeOriginDestination_duration_text": "$routeOriginDestination.duration.text",
                "routeOriginDestination_duration_value": {"$toString": "$routeOriginDestination.duration.value"},
            },
        },
        {
            "$unset": "_id",
        },
    ]


schema = Schema(
    {
        "id": string(),
        "createdAt": string(),
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "event": string(),
        "passenger": string(),
        "city": string(),
        "broadcastQtd": string(),
        "rating_score": string(),
        "isSuspect": string(),
        "isInvalid": string(),
        "status": string(),
        "car": string(),
        "driver": string(),
        "routeOriginDestination_distance_text": string(),
        "routeOriginDestination_distance_value": string(),
        "routeOriginDestination_duration_text": string(),
        "routeOriginDestination_duration_value": string(),
        "billing_estimatedPrice": string(),
        "billing_associatedPaymentMethod": string(),
        "billing_associatedTaximeter": string(),
        "billing_associatedMinimumFare": string(),
        "billing_associatedDiscount": string(),
        "billing_associatedCorporative_externalPropertyPassenger": string(),
        "billing_finalPrice_totalToPay": string(),
        "billing_finalPrice_totalPriceToll": string(),
        "billing_finalPrice_totalWithDiscount": string(),
        "billing_finalPrice_totalDiscount": string(),
        "billing_finalPrice_totalWithoutDiscount": string(),
        "billing_finalPrice_totalByTaximeter": string(),
        "billing_finalPrice_totalByStoppedTime": string(),
        "billing_finalPrice_totalByKm": string(),
        "billing_finalPrice_minPrice": string(),
    },
)
