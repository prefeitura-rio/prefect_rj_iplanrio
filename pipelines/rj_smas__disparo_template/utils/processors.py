# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Custom query processors for disparo template - migrado do Prefect 1.4
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/processors.py
Registry of functions that process queries at runtime
"""

from datetime import datetime

from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

from pipelines.rj_smas__disparo_template.constants import TemplateConstants  # pylint: disable=E0611, E0401


def process_cadunico_query(query: str = None) -> str:
    """
    Processes the CADUNICO query by substituting dynamic values.
    If no query provided, uses the one from constants.

    Args:
        query: Optional query string with placeholders. If None, uses constants.

    Returns:
        The processed query with substituted values

    Raises:
        ValueError: If {days_ahead} placeholder is not found in query
    """
    # Use query from constants if none provided
    if query is None:
        query = TemplateConstants.QUERY.value

    if "{days_ahead}" not in query:
        raise ValueError("Query must contain {days_ahead} placeholder for dynamic substitution")

    # Get dynamic days_ahead based on current weekday
    current_weekday = datetime.now().weekday()  # 0=Monday, 6=Sunday
    weekday_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    # Thursday (3) or Friday (4) = 4 days ahead
    if current_weekday in [3, 4]:  # quinta, sexta
        days_ahead = 4
        log(f"CADUNICO: Current day is {weekday_names[current_weekday]} - {days_ahead} days ahead")
    else:
        # Other days = 2 days ahead
        days_ahead = 2
        log(f"CADUNICO: Current day is {weekday_names[current_weekday]} - {days_ahead} days ahead")

    hsm_id = TemplateConstants.ID_HSM.value
    formatted_query = query.format(days_ahead=int(days_ahead), hsm_id=str(hsm_id))
    print(formatted_query)
    return formatted_query


# Registry of custom query processors
QUERY_PROCESSORS = {
    "cadunico": process_cadunico_query,
    # Future processors can be added here
}


def get_query_processor(processor_name: str):
    """Get query processor function by name"""
    return QUERY_PROCESSORS.get(processor_name)
