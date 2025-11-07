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


def process_skip_weekends_on_query(query: str = None, replacements: dict = {}) -> str:
    """
    Processes query by substituting dynamic values.
    If no query provided, uses the one from constants.

    Args:
        query: Optional query string with placeholders. If None, uses constants.
        replacements: dictionary with key "days_ahead_placeholder" that indicates how many days on the future are you looking at

    Returns:
        The processed query with substituted values

    Raises:
        ValueError: If {days_ahead_placeholder} placeholder is not found in query
    """
    # Use query from constants if none provided
    if query is None:
        query = TemplateConstants.QUERY.value

    if "{days_ahead_placeholder}" not in query:
        raise ValueError("Query must contain {days_ahead_placeholder} placeholder for dynamic substitution")

    if "days_ahead_placeholder" not in replacements:
        raise ValueError("Key 'days_ahead_placeholder' must exist on 'replacements' dictionary")

    days_ahead = int(replacements["days_ahead_placeholder"])

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

    # Saturday (5) or Sunday (6) = skip flow
    if current_weekday in [5, 6]:  # sábado, domingo
        return None

    # Thursday (3) or Friday (4) = 4 days ahead
    if current_weekday+days_ahead in [5, 6]:  # sábado, domingo
        days_ahead += 2
        replacements["days_ahead_placeholder"] = days_ahead
        new_day = (current_weekday+days_ahead)%7
        log(f"Current day is {weekday_names[current_weekday]} - Skipping to {weekday_names[new_day]}.")

    log(f"Current day is {weekday_names[current_weekday]} - {days_ahead} days ahead")

    formatted_query = query.format_map(replacements)
    print(f"formatted_query: {formatted_query}")
    return formatted_query


# Registry of custom query processors
QUERY_PROCESSORS = {
    "skip_weekends": process_skip_weekends_on_query,
    # Future processors can be added here
}


def get_query_processor(processor_name: str):
    """Get query processor function by name"""
    return QUERY_PROCESSORS.get(processor_name)
