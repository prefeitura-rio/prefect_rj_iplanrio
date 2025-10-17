# -*- coding: utf-8 -*-
"""
Query processors registry for rj_smas__disparo_pic

Provides a registry pattern for dynamic query transformations based on
business logic. Query processors allow runtime modification of SQL queries
before execution, enabling flexible data extraction workflows.

Each processor is a callable that takes a query string and returns a
transformed query string.
"""

from iplanrio.pipelines_utils.logging import log


def process_example_query(query: str) -> str:
    """
    Example query processor.

    This is a placeholder processor demonstrating the pattern. Replace or
    extend with actual business logic as needed.

    Args:
        query: SQL query string

    Returns:
        str: Transformed query string

    Example:
        >>> transformed = process_example_query("SELECT * FROM table")
        >>> print(transformed)
        "SELECT * FROM table"
    """
    log("Processing query with example processor")
    return query


# Registry of query processors
# Add new processors here following the same pattern
QUERY_PROCESSORS = {
    "example": process_example_query,
    # Future processors can be added here
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
