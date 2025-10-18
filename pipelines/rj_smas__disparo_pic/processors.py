# -*- coding: utf-8 -*-
"""
Query processors registry for rj_smas__disparo_pic

Provides a registry pattern for dynamic query transformations based on
business logic. Query processors allow runtime modification of SQL queries
before execution, enabling flexible data extraction workflows.

Each processor is a callable that takes a query string and returns a
transformed query string.
"""

from datetime import datetime, timedelta

from iplanrio.pipelines_utils.logging import log

from pipelines.rj_smas__disparo_pic.constants import PicConstants


def process_pic_query(query: str = None) -> str:
    """
    Processes the PIC query by substituting dynamic date values.

    Calculates the event date as D+2 (2 days ahead from today) and substitutes
    the {data_evento} placeholder in the query. This allows the pipeline to
    dynamically dispatch messages for events happening 2 days in the future.

    Args:
        query: Optional query string with {data_evento} placeholder.
               If None, uses query from PicConstants.

    Returns:
        str: The processed query with substituted date value

    Raises:
        ValueError: If {data_evento} placeholder is not found in query

    Example:
        >>> # For a query with placeholder:
        >>> query = "SELECT * FROM table WHERE event_date = '{data_evento}'"
        >>> result = process_pic_query(query)
        >>> # Result will have '{data_evento}' replaced with 'YYYY-MM-DD' (D+2)
    """
    # Use query from constants if none provided
    if query is None:
        query = PicConstants.PIC_QUERY.value

    if "{data_evento}" not in query:
        raise ValueError("Query must contain {data_evento} placeholder for dynamic substitution")

    # Calculate D+2 (2 days ahead)
    data_evento = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

    log(f"PIC: Calculated event date (D+2): {data_evento}")

    # Format query with calculated date
    formatted_query = query.format(data_evento=data_evento)

    return formatted_query


# Registry of query processors
# Add new processors here following the same pattern
QUERY_PROCESSORS = {
    "pic": process_pic_query,
}


def get_query_processor(processor_name: str):
    """
    Get a query processor function by name.

    Looks up and returns a query processor from the registry. Returns None
    if the processor name is not found.

    Args:
        processor_name: Name of the processor in the registry

    Returns:
        Callable query processor function, or None if not found

    Example:
        >>> processor = get_query_processor("example")
        >>> if processor:
        ...     query = processor("SELECT * FROM table")
    """
    return QUERY_PROCESSORS.get(processor_name)
