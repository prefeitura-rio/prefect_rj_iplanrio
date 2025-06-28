import random
from os import getenv

import pandas as pd
from prefect import flow, task


@task
def print_env():
    env_name = "EXAMPLE"
    print(f'Env {env_name} is set to "{getenv(env_name)}"')


@task
def use_pandas():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    print(df)


@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]


@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"


@flow(log_prints=True)
def rj_sec__example() -> list[str]:
    print_env()
    use_pandas()
    customer_ids = get_customer_ids()
    # Map the process_customer task across all customer IDs
    results = process_customer.map(customer_ids)
    return results
