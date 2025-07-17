# /// script
# dependencies = ["prefect>=3.4.3", "prefect-docker>=0.6.5"]
# ///

import asyncio
import logging
import sys
from os import environ
from pathlib import Path

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(levelname)s: %(message)s",
)


async def package_was_changed(package_dir: Path) -> bool:
    """Returns True if the package directory has changes in git compared to HEAD."""

    try:
        process = await asyncio.create_subprocess_exec(
            "git",
            "diff",
            "--name-only",
            "HEAD",
            package_dir.as_posix(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await process.communicate()
        return bool(stdout.decode().strip())
    except Exception as e:
        logging.warning(f"Could not determine git changes for `{package_dir}`: {e}")
        return True


async def deploy_flow(file: Path, environment: str, force: bool) -> tuple[Path, int]:
    """Deploy a Prefect flow defined in the given file to the specified environment."""

    package = file.parent.name

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        return file, 1

    if force:
        logging.warning(f"Force deploy is enabled. Deploying `{package}` regardless of changes.")
    else:
        logging.info(f"Checking if package `{package}` has changes...")
        changed = await package_was_changed(file.parent)
        if not changed:
            logging.info(f"No changes detected in `{package}`. Skipping deployment.")
            return file, 0

    logging.info(f"Deploying `{package}` {environment} pipelines...")

    name = package.replace("_", "-")

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
        f"{name}--{environment}",
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
    force_deploy = environ.get("FORCE_DEPLOY", "1").lower() == "1"

    if "ENVIRONMENT" not in environ:
        logging.warning("ENVIRONMENT variable not set, defaulting to 'staging'.")

    pipelines = Path(environ.get("PIPELINES_PATH", "pipelines"))

    if not pipelines.exists():
        logging.error(f"Pipelines path `{pipelines}` does not exist.")
        sys.exit(1)

    logging.info(f"Using pipelines path: `{pipelines}`")

    yamls = [d / "prefect.yaml" for d in pipelines.iterdir() if d.is_dir() and (d / "prefect.yaml").exists()]
    logging.info(f"Found {len(yamls)} flow(s) to deploy: {[str(y) for y in yamls]}")

    tasks = [deploy_flow(file, environment, force_deploy) for file in yamls]
    result = await asyncio.gather(*tasks)

    errors = [file for file, code in result if code != 0]

    if errors:
        logging.error(f"Deployment completed with errors in {len(errors)} flow(s): {[str(e) for e in set(errors)]}")
        sys.exit(1)


asyncio.run(main())
