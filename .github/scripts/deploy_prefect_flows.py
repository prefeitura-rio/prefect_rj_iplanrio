# /// script
# dependencies = ["prefect>=3.4.3", "prefect-docker>=0.6.5"]
# ///

import asyncio
import logging
from os import environ
from pathlib import Path
from sys import exit, stdout

logging.basicConfig(
    stream=stdout,
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


async def deploy_flow(file: Path, environment: str) -> tuple[str, bool]:
    package = file.parent.name

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        return package, False

    pipeline = f"{package}_{environment}" if environment == "staging" else file.parent.name

    logging.info(f"Deploying `{pipeline}` pipeline...")

    command = [
        "uv",
        "run",
        "--package",
        package,
        "--",
        "prefect",
        "--no-prompt",
        "deploy",
        "--name",
        pipeline,
        "--prefect-file",
        str(file),
    ]

    logging.debug(f"Running command: {' '.join(command)}")

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)

            logging.debug(f"Process stdout for `{pipeline}`:\n{stdout.decode()}")
            logging.debug(f"Process stderr for `{pipeline}`:\n{stderr.decode()}")
        except asyncio.TimeoutError:
            process.kill()
            return_code = await process.wait()

            logging.error(f"Deployment timed out for `{pipeline}`. Return code: {return_code}")

            return pipeline, False

        if process.returncode != 0:
            logging.error(f"Failed to deploy `{pipeline}`: exit code {process.returncode}. stderr:\n{stderr.decode()}")
            return pipeline, False

        logging.info(f"Successfully deployed `{pipeline}`.")
        return pipeline, True
    except Exception as e:
        logging.error(f"Unexpected error deploying `{pipeline}`: {e}")
        return pipeline, False


async def main():
    logging.info("Starting deployment of Prefect flows...")

    environment = environ.get("ENVIRONMENT", "staging")
    pipelines = Path(environ.get("PIPELINES_PATH", "pipelines"))

    logging.info(f"Using pipelines path: `{pipelines}`")
    logging.info(f"Using environment: `{environment}`")

    yamls = [dir / "prefect.yaml" for dir in pipelines.iterdir() if dir.is_dir()]
    logging.info(f"Found {len(yamls)} pipeline(s) to deploy: {[str(y) for y in yamls]}")

    tasks = [deploy_flow(file, environment) for file in yamls]
    results = await asyncio.gather(*tasks)
    failures = [name for name, success in results if not success]

    if failures:
        logging.error(f"Deployments failed for: `{'`, `'.join(failures)}`")
        exit(1)

    logging.info("All deployments completed successfully.")


asyncio.run(main())
