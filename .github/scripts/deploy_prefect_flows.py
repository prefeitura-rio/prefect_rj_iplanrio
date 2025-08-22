# -*- coding: utf-8 -*-
# /// script
# dependencies = ["prefect>=3.4.3", "prefect-docker>=0.6.5", "pyyaml>=6.0.2", "uvloop>=0.21.0"]
# ///

import logging
import sys
from collections.abc import Iterable
from functools import partial
from os import environ
from pathlib import Path

import uvloop
from yaml import safe_load

logging.basicConfig(
    stream=sys.stdout,
    level=environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s: %(message)s",
)

# TODO: add Pydantic model for `prefect.yaml` validation


async def get_deployments(prefect_yaml: Path) -> list[str]:
    """Extract deployment names from the given `prefect.yaml` file."""

    content = safe_load(prefect_yaml.read_text())

    deployments_section = content.get("deployments")

    if not deployments_section:
        logging.warning(f"No deployments section found in `{prefect_yaml}`.")
        return []

    return [d.get("name") for d in deployments_section]


async def get_prefect_yaml_files(package_dir: Iterable[Path]) -> list[Path]:
    """Get all `prefect.yaml` files in the specified package directory."""

    return [d / "prefect.yaml" for d in package_dir if d.is_dir() and (d / "prefect.yaml").exists()]


async def get_changed_directories(package_dir: Path, sha: str) -> list[Path]:
    """Get directories that have changed since the specified commit SHA."""

    command = [
        "git",
        "diff",
        "--name-only",
        f"{sha}^",
        sha,
        "--",
        package_dir.as_posix(),
    ]

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, _ = await process.communicate()

    parsed_stdout = stdout.decode().strip().splitlines()

    logging.debug(f"Command: `{' '.join(command)}`")
    logging.debug(f"STDOUT: {parsed_stdout}")

    files = {Path(f).parent for f in parsed_stdout}

    return list(files)


async def deploy_flow(file: Path, environment: str) -> tuple[Path, int]:
    """Deploy a Prefect flow defined in the given file to the specified environment."""

    package = file.parent.name

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        return file, 1

    logging.info(f"Deploying `{package}` {environment} pipelines...")

    deployments = await get_deployments(file)

    try:
        deployment = next(filter(lambda d: d.endswith(environment), deployments))
    except StopIteration:
        logging.warning(f"No deployment found for `{package}` in `{environment}` environment. Skipping.")
        return file, 0

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
        deployment,
        "--prefect-file",
        str(file),
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)
        except asyncio.TimeoutError:
            process.kill()
            _ = await process.wait()
            logging.error("Deployment timed out.")
            return file, 1

        if process.returncode != 0:
            logging.error(f"Failed to deploy `{package}` to {environment} environment.")
            logging.debug(f"Command: `{' '.join(command)}`")
            logging.debug(f"Exit code: {process.returncode}")
            logging.debug(f"STDOUT: {stdout.decode().strip()}")
            logging.debug(f"STDERR: {stderr.decode().strip()}")
            return file, 1

        logging.info(f"Successfully deployed `{package}` to {environment} environment.")
        return file, 0
    except Exception as e:
        logging.error(f"An error occurred while deploying `{file}`: {e}")
        return file, 1


async def main() -> None:
    """Main entrypoint for deploying all Prefect flows."""

    logging.info("Starting deployment of Prefect flows...")

    environment = environ.get("ENVIRONMENT", "staging")
    force_deploy = environ.get("FORCE_DEPLOY", "0") == "1"
    sha = environ.get("GITHUB_SHA", "HEAD")

    logging.debug(f"Environment: `{environment}`")
    logging.debug(f"Force deploy: `{force_deploy}`")
    logging.debug(f"SHA: `{sha}`")

    pipelines = Path(environ.get("PIPELINES_PATH", "pipelines"))

    if not pipelines.exists():
        logging.error(f"Path `{pipelines}` does not exist.")
        sys.exit(1)

    changed_pipelines = await get_changed_directories(pipelines, sha)

    if not changed_pipelines:
        if force_deploy:
            yamls = await get_prefect_yaml_files(pipelines.iterdir())
        else:
            logging.info("No changes detected, skipping deployment.")
            sys.exit(0)
    else:
        yamls = await get_prefect_yaml_files(changed_pipelines)

    logging.info(f"Found {len(yamls)} flow(s) to deploy: {[str(y) for y in yamls]}")

    deploy_flow_with_environment = partial(deploy_flow, environment=environment)

    result = await asyncio.gather(*[deploy_flow_with_environment(file) for file in yamls])

    errors = [file for file, code in result if code != 0]

    if errors:
        logging.error(f"Deployment completed with errors in {len(errors)} flow(s): {[str(e) for e in set(errors)]}")
        sys.exit(1)


uvloop.run(main())
