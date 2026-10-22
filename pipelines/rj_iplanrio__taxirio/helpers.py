"""The PyMongoArrow schemas of the results of the TaxiRio pipelines, one for each collection."""

from pyarrow import string
from pymongoarrow.api import Schema

CITIES = Schema(
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

DISCOUNTS = Schema(
    {
        "id": string(),
        "description": string(),
        "value": string(),
        "createdAt": string(),
    },
)

DRIVERS = Schema(
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

METRICSDRIVERUNOCCUPIEDS = Schema(
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

PASSENGERS = Schema(
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

PAYMENTMETHODS = Schema(
    {
        "id": string(),
        "pindex": string(),
        "name": string(),
        "type": string(),
    },
)

RACES = Schema(
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

RANKINGRACES = Schema(
    {
        "id": string(),
        "createdAt": string(),
        "updatedAt": string(),
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "race": string(),
        "competitors": string(),
    },
)

USERS = Schema(
    {
        "id": string(),
        "displayName": string(),
        "fullName": string(),
        "email": string(),
        "phoneNumber": string(),
        "cpf": string(),
        "createdAt": string(),
        "birthDate": string(),
        "validadoReceita": string(),
        "ano_particao": string(),
        "mes_particao": string(),
        "dia_particao": string(),
        "federalRevenueData_name": string(),
        "federalRevenueData_birthDate": string(),
        "federalRevenueData_mothersName": string(),
        "federalRevenueData_yearOfDeath": string(),
        "federalRevenueData_phone": string(),
        "federalRevenueData_sex": string(),
    },
)
