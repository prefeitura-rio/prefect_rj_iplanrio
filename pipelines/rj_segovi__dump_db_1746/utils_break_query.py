# -*- coding: utf-8 -*-
# ruff: noqa

from datetime import datetime, timedelta
from typing import List, Optional
from uuid import uuid4

import basedosdados as bd
import pytz
from iplanrio.pipelines_utils.bd import get_storage_blobs
from iplanrio.pipelines_utils.gcs import parse_blobs_to_partition_dict
from iplanrio.pipelines_utils.io import extract_last_partition_date
from iplanrio.pipelines_utils.logging import log


def format_partitioned_query(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: Optional[List[str]] = None,
    lower_bound_date: Optional[str] = None,
    date_format: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    wait: Optional[str] = None,
) -> List[dict]:
    """
    Formats a query for fetching partitioned data.
    """
    if not partition_columns or partition_columns[0] == "":
        log("NO partition column specified. Returning query as is")
        return [{"query": query, "start_date": None, "end_date": None}]

    partition_column = partition_columns[0]
    last_partition_date = get_last_partition_date(dataset_id, table_id, date_format)

    if last_partition_date is None:
        log("NO partition blob was found.")

    # Check if the table already exists in BigQuery.
    table = bd.Table(dataset_id, table_id)

    # If it doesn't, return the query as is, so we can fetch the whole table.
    if not table.table_exists(mode="staging"):
        log("NO tables was found.")

    if not break_query_frequency:
        return [
            build_single_partition_query(
                query=query,
                partition_column=partition_column,
                lower_bound_date=lower_bound_date,
                last_partition_date=last_partition_date,
                date_format=date_format,
                database_type=database_type,
            )
        ]

    return build_chunked_queries(
        query=query,
        partition_column=partition_column,
        date_format=date_format,
        database_type=database_type,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
        lower_bound_date=lower_bound_date,
        last_partition_date=last_partition_date,
    )


def get_last_partition_date(dataset_id: str, table_id: str, date_format: Optional[str]) -> Optional[str]:
    blobs = get_storage_blobs(dataset_id=dataset_id, table_id=table_id)
    storage_partitions_dict = parse_blobs_to_partition_dict(blobs=blobs)
    return extract_last_partition_date(partitions_dict=storage_partitions_dict, date_format=date_format)


def get_last_date(lower_bound_date: Optional[str], date_format: str, last_partition_date: str) -> str:
    brazil_timezone = pytz.timezone("America/Sao_Paulo")
    now: datetime = datetime.now(brazil_timezone)
    if lower_bound_date == "current_year":
        return now.replace(month=1, day=1).strftime(date_format)
    elif lower_bound_date == "current_month":
        return now.replace(day=1).strftime(date_format)
    elif lower_bound_date == "current_day":
        return now.strftime(date_format)
    elif lower_bound_date:
        if last_partition_date:
            return min(
                datetime.strptime(lower_bound_date, date_format),
                datetime.strptime(last_partition_date, date_format),
            ).strftime(date_format)
        else:
            return datetime.strptime(lower_bound_date, date_format).strftime(date_format)
    return datetime.strptime(last_partition_date, date_format).strftime(date_format)


def build_single_partition_query(
    query: str,
    partition_column: str,
    lower_bound_date: Optional[str],
    last_partition_date: str,
    date_format: str,
    database_type: str,
) -> dict:
    last_date = get_last_date(
        lower_bound_date=lower_bound_date,
        date_format=date_format,
        last_partition_date=last_partition_date,
    )
    aux_name = f"a{uuid4().hex}"[:8]

    log(f"Partitioned DETECTED: {partition_column}, returning a NEW QUERY with partitioned columns and filters")

    if database_type == "oracle":
        oracle_date_format = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{last_date}', '{oracle_date_format}')
        """
    elif database_type in ["mysql", "postgres", "sql_server"]:
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where CONVERT(DATE, {partition_column}) >= '{last_date}'
        """
    else:
        raise ValueError(f"Unsupported database type: {database_type}")

    return {
        "query": query,
        "start_date": last_date,
        "end_date": last_date,
    }


def build_chunked_queries(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    break_query_start: Optional[str],
    break_query_end: Optional[str],
    break_query_frequency: Optional[str],
    lower_bound_date: Optional[str],
    last_partition_date: str,
) -> List[dict]:
    start_date_str = get_last_date(
        lower_bound_date=break_query_start,
        date_format=date_format,
        last_partition_date=None,
    )
    end_date_str = get_last_date(
        lower_bound_date=break_query_end,
        date_format=date_format,
        last_partition_date=None,
    )
    end_date = datetime.strptime(end_date_str, date_format)

    if break_query_end == "current_month":
        end_date = get_last_day_of_month(date=end_date)
        end_date_str = end_date.strftime(date_format)
    elif break_query_end == "current_year":
        end_date = get_last_day_of_year(year=end_date.year)
        end_date_str = end_date.strftime(date_format)

    log("Breaking query into multiple chunks based on frequency")
    log(f"    break_query_frequency: {break_query_frequency}")
    log(f"    break_query_start: {start_date_str}")
    log(f"    break_query_end: {end_date_str}")

    current_start = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    queries = []

    while current_start <= end_date:
        current_end = calculate_end_date(
            current_start=current_start,
            end_date=end_date,
            break_query_frequency=break_query_frequency,
        )
        queries.append(
            build_chunk_query(
                query=query,
                partition_column=partition_column,
                date_format=date_format,
                database_type=database_type,
                current_start=current_start,
                current_end=current_end,
            )
        )
        current_start = get_next_start_date(current_start=current_start, break_query_frequency=break_query_frequency)

    log(f"Total queries created: {len(queries)}")
    return queries


def calculate_end_date(current_start: datetime, end_date: datetime, break_query_frequency: Optional[str]) -> datetime:
    if break_query_frequency.lower() == "month":
        return min(get_last_day_of_month(date=current_start), end_date)
    elif break_query_frequency.lower() == "year":
        return min(get_last_day_of_year(year=current_start.year), end_date)
    elif break_query_frequency.lower() == "day":
        return min(current_start, end_date)
    elif break_query_frequency.lower() == "week":
        return min(current_start + timedelta(days=6), end_date)
    elif break_query_frequency.lower() == "bimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=2)),
            end_date,
        )
    elif break_query_frequency.lower() == "trimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=3)),
            end_date,
        )
    elif break_query_frequency.lower() == "quadrimester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=4)),
            end_date,
        )
    elif break_query_frequency.lower() == "semester":
        return min(
            get_last_day_of_month(date=add_months(start_date=current_start, months=6)),
            end_date,
        )
    else:
        raise ValueError(
            f"Unsupported break_query_frequency: {break_query_frequency}. Use one of the following: year, month, day, week, bimester, trimester, quadrimester and semester"  # noqa
        )


def build_chunk_query(
    query: str,
    partition_column: str,
    date_format: str,
    database_type: str,
    current_start: datetime,
    current_end: datetime,
) -> dict:
    aux_name = f"a{uuid4().hex}"[:8]

    if database_type == "oracle":
        oracle_date_format: str = "YYYY-MM-DD" if date_format == "%Y-%m-%d" else date_format
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where {partition_column} >= TO_DATE('{current_start.strftime(date_format)}', '{oracle_date_format}')
            and {partition_column} <= TO_DATE('{current_end.strftime(date_format)}', '{oracle_date_format}')
        """
    elif database_type in ["mysql", "postgres", "sql_server"]:
        query = f"""
        with {aux_name} as ({query})
        select * from {aux_name}
        where CONVERT(DATE, {partition_column}) >= '{current_start.strftime(date_format)}'
            and CONVERT(DATE, {partition_column}) <= '{current_end.strftime(date_format)}'
        """
    else:
        raise ValueError(f"Unsupported database type: {database_type}")

    return {
        "query": query,
        "start_date": current_start.strftime(date_format),
        "end_date": current_end.strftime(date_format),
    }


def get_next_start_date(current_start: datetime, break_query_frequency: Optional[str]) -> datetime:
    if break_query_frequency.lower() == "month":
        return add_months(start_date=current_start, months=1)
    elif break_query_frequency.lower() == "year":
        return datetime(current_start.year + 1, 1, 1)
    elif break_query_frequency.lower() == "day":
        return current_start + timedelta(days=1)
    elif break_query_frequency.lower() == "week":
        return current_start + timedelta(days=7)
    elif break_query_frequency.lower() in [
        "bimester",
        "trimester",
        "quadrimester",
        "semester",
    ]:
        months_to_add = {
            "bimester": 2,
            "trimester": 3,
            "quadrimester": 4,
            "semester": 6,
        }
        return add_months(
            start_date=current_start,
            months=months_to_add[break_query_frequency.lower()],
        )
    return current_start


def get_last_day_of_month(date: datetime) -> datetime:
    next_month = date.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def get_last_day_of_year(year: int) -> datetime:
    return datetime(year, 12, 31)


def add_months(start_date: datetime, months: int) -> datetime:
    new_month = start_date.month + months
    year_increment = (new_month - 1) // 12
    new_month = (new_month - 1) % 12 + 1
    new_year = start_date.year + year_increment
    return datetime(new_year, new_month, start_date.day)
