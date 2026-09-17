# -*- coding: utf-8 -*-
from pyarrow import string
from pymongoarrow.api import Schema

pipeline = [
    {
        "$project": {
            "id": {"$toString": "$_id"},
            "createdAt": {"$dateToString": {"date": "$createdAt"}},
            "ano_particao": {"$dateToString": {"format": "%Y", "date": "$createdAt"}},
            "mes_particao": {"$dateToString": {"format": "%m", "date": "$createdAt"}},
            "dia_particao": {"$dateToString": {"format": "%d", "date": "$createdAt"}},
            "displayName": 1,
            "fullName": 1,
            "email": 1,
            "phoneNumber": 1,
            "cpf": 1,
            "validadoReceita": {"$toString": "$validadoReceita"},
            "federalRevenueData_name": "$federalRevenueData.name",
            "federalRevenueData_birthDate": "$federalRevenueData.birthDate",
            "federalRevenueData_mothersName": "$federalRevenueData.mothersName",
            "federalRevenueData_yearOfDeath": {"$toString": "$federalRevenueData.yearOfDeath"},
            "federalRevenueData_phone": "$federalRevenueData.phone",
            "federalRevenueData_sex": "$federalRevenueData.sex",
        },
    },
    {
        "$unset": "_id",
    },
]

schema = Schema(
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
