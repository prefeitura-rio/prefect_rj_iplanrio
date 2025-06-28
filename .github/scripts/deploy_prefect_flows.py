# /// script
# dependencies = ["prefect>=3.4.3", "prefect-docker>=0.6.5"]
# ///

import logging
from os import environ
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired, run
from sys import exit, stdout

path = environ.get("PIPELINES_PATH", "pipelines")
pipelines = Path(path)
failures: list[str] = []

logging.basicConfig(
    stream=stdout,
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

yamls = (dir / "prefect.yaml" for dir in pipelines.iterdir() if dir.is_dir())

logging.info("Starting deployment of Prefect flows...")
logging.info(f"Using pipelines path: {pipelines}")

for file in yamls:
    logging.info(f"Deploying `{file.parent.name}`...")

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        failures.append(file.parent.name)
        continue

    command = [
        "uv",
        "run",
        "--active",
        "--",
        "prefect",
        "--no-prompt",
        "deploy",
        "--name",
        str(file.parent.name),
        "--prefect-file",
        str(file),
    ]

    try:
        result = run(command, check=True, capture_output=True, text=True, timeout=120)
        logging.info(f"Successfully deployed `{file.parent.name}`.")
        logging.info(f"Result:\n{result.stdout}")
    except CalledProcessError as e:
        logging.error(f"Failed to deploy `{file.parent.name}`: {e}")
        logging.error(f"stderr:\n{e.stderr}")
        failures.append(file.parent.name)
    except TimeoutExpired:
        logging.error(f"Deployment timed out for `{file.parent.name}`")
        failures.append(file.parent.name)
    except Exception as e:
        logging.error(f"Unexpected error deploying `{file.parent.name}`: {e}")
        failures.append(file.parent.name)

if failures:
    logging.error(f"Deployments failed for: `{'`, `'.join(failures)}`")
    exit(1)

logging.info("All deployments completed successfully.")
