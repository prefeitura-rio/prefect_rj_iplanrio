# /// script
# dependencies = ["prefect>=3.4.3", "prefect-docker>=0.6.5", "pyyaml>=6.0.2", "uvloop>=0.21.0"]
# ///

import asyncio
import logging
import sys
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass, field
from functools import wraps
from itertools import batched
from os import environ
from pathlib import Path
from typing import Callable, TypedDict

import uvloop
from yaml import safe_load

logging.basicConfig(
    stream=sys.stdout,
    level=environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s: %(message)s",
)

DOCKER_IMAGE_FORMAT_PARTS_COUNT = 2
BASE_IMAGE = "ghcr.io/prefeitura-rio/prefect_rj_iplanrio:latest"

DeploymentFunc = Callable[[Path, str, int, int], Awaitable[tuple[Path, int]]]
DeploymentWrapper = Callable[[Path, str], Awaitable[tuple[Path, int]]]


def safe_int_from_env(key: str, default: int, min_value: int = 0) -> int:
    """Safely parse integer from environment variable with validation."""
    try:
        value = int(environ.get(key, str(default)))
        return max(min_value, value)
    except ValueError:
        logging.warning(f"Invalid value for {key}, using default: {default}")
        return default


def safe_float_from_env(key: str, default: float, min_value: float = 0.0) -> float:
    """Safely parse float from environment variable with validation."""
    try:
        value = float(environ.get(key, str(default)))
        return max(min_value, value)
    except ValueError:
        logging.warning(f"Invalid value for {key}, using default: {default}")
        return default


@dataclass
class DeploymentConfig:
    """Configuration for deployment script."""

    environment: str = field(default_factory=lambda: environ.get("ENVIRONMENT", "staging"))
    force_deploy: bool = field(default_factory=lambda: environ.get("FORCE_DEPLOY", "0") == "1")
    sha: str = field(default_factory=lambda: environ.get("GITHUB_SHA", "HEAD"))
    batch_size: int = field(default_factory=lambda: safe_int_from_env("BATCH_SIZE", 3, 1))
    cleanup_docker: bool = field(default_factory=lambda: environ.get("CLEANUP_DOCKER", "0") == "1")
    max_retries: int = field(default_factory=lambda: safe_int_from_env("MAX_RETRIES", 2, 0))
    deployment_timeout: int = field(default_factory=lambda: safe_int_from_env("DEPLOYMENT_TIMEOUT", 600, 30))
    retry_delay: int = field(default_factory=lambda: safe_int_from_env("RETRY_DELAY", 30, 1))
    backoff_multiplier: float = field(default_factory=lambda: safe_float_from_env("BACKOFF_MULTIPLIER", 2.0, 1.0))


CONFIG = DeploymentConfig()


class WorkPool(TypedDict, total=False):
    """Work pool configuration."""

    name: str
    work_queue_name: str
    job_variables: dict[str, str | int | bool]


class Schedule(TypedDict, total=False):
    """Schedule configuration."""

    cron: str
    timezone: str
    active: bool


class BuildStep(TypedDict, total=False):
    """Build step configuration."""

    prefect_docker: dict[str, str]
    run_shell_script: dict[str, str]


class PrefectDeployment(TypedDict, total=False):
    """Represents a deployment configuration in prefect.yaml."""

    name: str
    entrypoint: str
    parameters: dict[str, str | int | bool | list[str]]
    work_pool: WorkPool
    schedule: Schedule


class PrefectYaml(TypedDict, total=False):
    """Represents the structure of a prefect.yaml file."""

    name: str
    prefect_version: str
    build: list[BuildStep]
    push: list[dict[str, str]]
    pull: list[dict[str, str]]
    deployments: list[PrefectDeployment]


async def get_deployments(prefect_yaml: Path) -> list[str]:
    """Extract deployment names from the given `prefect.yaml` file."""
    try:
        content: PrefectYaml = safe_load(prefect_yaml.read_text()) or {}
    except (OSError, IOError) as e:
        logging.error(f"Failed to read `{prefect_yaml}`: {e}")
        return []
    except Exception as e:
        logging.error(f"Failed to parse YAML in `{prefect_yaml}`: {e}")
        return []

    deployments_section: list[PrefectDeployment] = content.get("deployments", [])

    if not deployments_section:
        logging.warning(f"No deployments section found in `{prefect_yaml}`.")
        return []

    return [d.get("name", "") for d in deployments_section if d.get("name")]


async def get_prefect_yaml_files(package_dir: Iterable[Path]) -> list[Path]:
    """Get all `prefect.yaml` files in the specified package directory."""
    return [d / "prefect.yaml" for d in package_dir if d.is_dir() and (d / "prefect.yaml").exists()]


async def get_changed_directories(package_dir: Path, sha: str) -> list[Path]:
    """Get directories that have changed since the specified commit SHA."""
    command = ["git", "diff", "--name-only", f"{sha}^", sha, "--", package_dir.as_posix()]

    process = await run_subprocess(command)

    stdout, _ = await process.communicate()

    parsed_stdout = stdout.decode().strip().splitlines()

    logging.debug(f"Command: `{' '.join(command)}`")
    logging.debug(f"STDOUT: {parsed_stdout}")

    files = {Path(f).parent for f in parsed_stdout}

    return list(files)


def parse_image_line(line: str) -> str | None:
    """Parse a Docker image line and return the image ID if valid."""
    parts = line.strip().split()
    return parts[1] if len(parts) >= DOCKER_IMAGE_FORMAT_PARTS_COUNT else None


async def get_docker_image_ids() -> list[str]:
    """Get list of Docker image IDs excluding the base image."""
    list_cmd = ["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"]

    process = await run_subprocess(list_cmd)

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        logging.warning(f"Failed to list Docker images: {stderr.decode().strip()}")
        return []

    images_output = stdout.decode().strip()

    if not images_output:
        return []

    return [
        image_id
        for line in images_output.splitlines()
        if line.strip() and not line.startswith(BASE_IMAGE)
        for image_id in [parse_image_line(line)]
        if image_id is not None
    ]


async def remove_docker_images(image_ids: list[str]) -> None:
    """Remove specified Docker images."""
    remove_cmd = ["docker", "rmi", "-f", *image_ids]

    process = await run_subprocess(remove_cmd)

    _, stderr = await process.communicate()

    if process.returncode != 0:
        logging.warning(f"Some images could not be removed: {stderr.decode().strip()}")
        return

    logging.info("Docker images cleaned up successfully")


async def cleanup_docker_system() -> None:
    """Clean up Docker build cache and unused resources."""
    commands = [["docker", "builder", "prune", "-f"], ["docker", "system", "prune", "-f"]]

    for cmd in commands:
        process = await run_subprocess(cmd)

        _ = await process.communicate()


def build_deployment_command(package: str, deployment: str, file_path: str) -> list[str]:
    """Build deployment command array."""
    return [
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
        file_path,
    ]


def is_retryable_error(stderr_text: str) -> bool:
    """Check if error is retryable based on stderr content."""
    return any(error in stderr_text.lower() for error in ["timeout", "connection", "network", "read timed out"])


async def run_subprocess(command: list[str]) -> asyncio.subprocess.Process:
    """Run subprocess with standard configuration."""
    return await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def cleanup_docker_images() -> None:
    """Clean up Docker images except the base image to free disk space."""
    try:
        logging.info("Starting Docker cleanup to free disk space...")

        image_ids = await get_docker_image_ids()

        if not image_ids:
            logging.info("No Docker images to clean up")
            return

        logging.info(f"Removing {len(image_ids)} Docker images...")

        await remove_docker_images(image_ids)
        await cleanup_docker_system()

        logging.info("Docker cleanup completed")

    except Exception as e:
        logging.warning(f"Docker cleanup failed but continuing deployment: {e}")


def retry_on_failure(func: DeploymentFunc) -> DeploymentWrapper:
    """Decorator to retry deployment functions with configurable exponential backoff.

    Uses CONFIG.retry_delay as base delay and CONFIG.backoff_multiplier for exponential growth.
    Delay formula: retry_delay * (backoff_multiplier ** attempt)
    """

    @wraps(func)
    async def wrapper(file: Path, environment: str) -> tuple[Path, int]:
        max_retries = CONFIG.max_retries

        for attempt in range(max_retries + 1):
            try:
                result: tuple[Path, int] = await func(file, environment, attempt, CONFIG.deployment_timeout)

                if result[1] == 0:
                    return result

                if attempt < max_retries:
                    delay = CONFIG.retry_delay * (CONFIG.backoff_multiplier**attempt)
                    logging.warning(f"Deployment failed, retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    return result

            except Exception as e:
                if attempt < max_retries:
                    delay = CONFIG.retry_delay * (CONFIG.backoff_multiplier**attempt)
                    logging.warning(f"Exception during deployment: {e}. Retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Deployment failed after {max_retries} retries: {e}")
                    return file, 1
        return file, 1

    return wrapper


@retry_on_failure
async def deploy_flow(file: Path, environment: str, retry_count: int = 0, timeout: int = 600) -> tuple[Path, int]:
    """Deploy a Prefect flow with retry logic."""
    package = file.parent.name

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        return file, 1

    retry_msg = f" (retry {retry_count})" if retry_count > 0 else ""
    logging.info(f"Deploying `{package}` {environment} pipelines{retry_msg}...")

    deployments = await get_deployments(file)

    try:
        deployment = next(d for d in deployments if d.endswith(environment))
    except StopIteration:
        logging.warning(f"No deployment found for `{package}` in `{environment}` environment. Skipping.")
        return file, 0

    command = build_deployment_command(package, deployment, str(file))

    process = await run_subprocess(command)

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        _ = await process.wait()
        raise TimeoutError(f"Deployment timed out for `{package}`") from None

    if process.returncode != 0:
        stderr_text = stderr.decode().strip()

        if is_retryable_error(stderr_text):
            raise RuntimeError(f"Network error deploying `{package}`: {stderr_text}")

        logging.error(f"Failed to deploy `{package}` to {environment} environment.")
        logging.error(f"Non-retryable error for `{package}`: {stderr_text}")
        logging.debug(f"Command: `{' '.join(command)}`")
        logging.debug(f"Exit code: {process.returncode}")
        logging.debug(f"STDOUT: {stdout.decode().strip()}")
        logging.debug(f"STDERR: {stderr.decode().strip()}")
        return file, 1

    logging.info(f"Successfully deployed `{package}` to {environment} environment.")
    return file, 0


async def main() -> None:
    """Main entrypoint for deploying all Prefect flows."""
    logging.info("Starting deployment of Prefect flows...")

    config = CONFIG

    logging.debug(f"Environment: `{config.environment}`")
    logging.debug(f"Force deploy: `{config.force_deploy}`")
    logging.debug(f"SHA: `{config.sha}`")
    logging.debug(f"Batch size: `{config.batch_size}`")
    logging.debug(f"Docker cleanup: `{config.cleanup_docker}`")
    logging.debug(f"Max retries: `{config.max_retries}`")
    logging.debug(f"Deployment timeout: `{config.deployment_timeout}`")
    logging.debug(f"Retry delay: `{config.retry_delay}`")
    logging.debug(f"Backoff multiplier: `{config.backoff_multiplier}`")

    pipelines_path = environ.get("PIPELINES_PATH", "pipelines")
    if not pipelines_path:
        logging.error("PIPELINES_PATH cannot be empty")
        sys.exit(1)

    pipelines = Path(pipelines_path)

    if not pipelines.exists():
        logging.error(f"Path `{pipelines}` does not exist.")
        sys.exit(1)

    changed_pipelines = await get_changed_directories(pipelines, config.sha)

    if not changed_pipelines and not config.force_deploy:
        logging.info("No changes detected, skipping deployment.")
        sys.exit(0)

    yamls = await get_prefect_yaml_files(changed_pipelines if changed_pipelines else pipelines.iterdir())

    logging.info(f"Found {len(yamls)} flow(s) to deploy: {[str(y) for y in yamls]}")

    all_errors: list[Path] = []

    batches = list(batched(yamls, config.batch_size, strict=False))

    for batch_num, batch in enumerate(batches, 1):
        logging.info(f"Deploying batch {batch_num}/{len(batches)}: {[str(y) for y in batch]}")

        tasks = [deploy_flow(file, config.environment) for file in batch]
        result: list[tuple[Path, int]] = await asyncio.gather(*tasks)
        all_errors.extend(file for file, code in result if code != 0)

        if config.cleanup_docker and batch_num < len(batches):
            await cleanup_docker_images()

    if all_errors:
        errors = [str(e) for e in set(all_errors)]
        logging.error(f"Deployment completed with errors in {len(all_errors)} flow(s): {errors}")
        sys.exit(1)


uvloop.run(main())