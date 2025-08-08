# -*- coding: utf-8 -*-
import asyncio
from typing import Literal

import aiohttp
import prefect
from discord import AllowedMentions, Embed, File, Webhook
from prefect.context import get_run_context

import os 
import re
import pandas as pd
from iplanrio.pipelines_utils.logging import log

from dbt.contracts.results import RunResult, SourceFreshnessResult
from google.cloud import storage


def get_environment():
    return prefect.context.get("parameters").get("environment")


# DISCORD - UTILS

async def send_discord_webhook(
    text_content: str,
    file_path: str = None,
    username: str = None,
    destination: str = "dbt-runs",
):
    """
    Sends a message to a Discord webhook

    Args:
        text_content (str): The message to send.
        file_path (str, optional): Path to file to attach. Defaults to None.
        username (str, optional): The username to use when sending the message. Defaults to None.
        destination (str, optional): Destination environment for the message. Defaults to "dbt-runs".
    """
    # Select webhook URL based on destination
    if destination == "notifications":
        webhook_url = os.getenv("DBT-RUN__DISCORD_WEBHOOK_URL_NOTIFICATIONS")
    elif destination == "incidentes":
        webhook_url = os.getenv("DBT-RUN__DISCORD_WEBHOOK_URL_INCIDENTES")
    else:
        # Default to notifications if destination is not recognized
        webhook_url = os.getenv("DBT-RUN__DISCORD_WEBHOOK_URL_NOTIFICATIONS")

    if len(text_content) > 2000:
        raise ValueError(f"Message content is too long: {len(text_content)} > 2000 characters.")

    async with aiohttp.ClientSession() as session:
        kwargs = {"content": text_content, "allowed_mentions": AllowedMentions(users=True)}
        if username:
            kwargs["username"] = username
        if file_path:
            file = File(file_path, filename=file_path)

            if ".png" in file_path:
                embed = Embed()
                embed.set_image(url=f"attachment://{file_path}")
                kwargs["embed"] = embed

            kwargs["file"] = file

        webhook = Webhook.from_url(webhook_url, session=session)
        try:
            await webhook.send(**kwargs)
        except RuntimeError:
            raise ValueError(f"Error sending message to Discord webhook: {webhook_url}")


def send_message(
    title, message, flow_info: dict, file_path=None, username=None, destination: str = "notifications"
):
    """
    Sends a message with the given title and content to a webhook.

    Args:
        title (str): The title of the message.
        message (str): The content of the message.
        flow_info (dict): Dictionary containing flow information.
        file_path (str, optional): Path to file to attach. Defaults to None.
        username (str, optional): The username to be used for the webhook. Defaults to None.
        destination (str, optional): Destination environment for the message ("notifications", "incidentes", etc.). Defaults to "notifications".
    """

    flow_environment = flow_info["flow_environment"]
    flow_name = flow_info["flow_name"]
    flow_run_id = flow_info["flow_run_id"]
    
    header_content = f"""
## {title}
> Prefect Environment: {flow_environment}
> Flow Run: [{flow_name}](https://prefect.squirrel-regulus.ts.net/runs/flow-run/{flow_run_id})
    """
    # Calculate max char count for message
    message_max_char_count = 2000 - len(header_content)

    # Split message into lines
    message_lines = message.split("\n")

    # Split message into pages
    pages = []
    current_page = ""
    for line in message_lines:
        if len(current_page) + 2 + len(line) < message_max_char_count:
            current_page += "\n" + line
        else:
            pages.append(current_page)
            current_page = line

    # Append last page
    pages.append(current_page)

    # Build message content using Header in first page
    message_contents = []
    for page_idx, page in enumerate(pages):
        if page_idx == 0:
            message_contents.append(header_content + page)
        else:
            message_contents.append(page)

    # Send message to Discord
    async def main(contents):
        for i, content in enumerate(contents):
            await send_discord_webhook(
                text_content=content,
                file_path=file_path if i == len(contents) - 1 else None,
                username=username,
                destination=destination
            )

    asyncio.run(main(message_contents))



# DBT - UTILS

def process_dbt_logs(log_path: str = "dbt_repository/logs/dbt.log") -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries

    Args:
        log_path (str): The path to the dbt log file. Defaults to "dbt_repository/logs/dbt.log".

    Returns:
        pd.DataFrame: A DataFrame containing the parsed log entries.
    """

    with open(log_path, "r", encoding="utf-8", errors="ignore") as log_file:
        log_content = log_file.read()

    result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
    parts = [part.strip() for part in result][1:]

    splitted_log = []
    for i in range(0, len(parts), 2):
        time = parts[i].replace(r"\x1b[0m", "")
        level = parts[i + 1][1:6].replace(" ", "")
        text = parts[i + 1][7:]
        splitted_log.append((time, level, text))

    full_logs = pd.DataFrame(splitted_log, columns=["time", "level", "text"])

    return full_logs


def log_to_file(logs: pd.DataFrame, levels=None) -> str:
    """
    Writes the logs to a file and returns the file path.

    Args:
        logs (pd.DataFrame): The logs to be written to the file.
        levels (list): The levels of logs to be written to the file.

    Returns:
        str: The file path of the generated log file.
    """
    if levels is None:
        levels = ["info", "error", "warn"]
    logs = logs[logs.level.isin(levels)]

    report = []
    for _, row in logs.iterrows():
        report.append(f"{row['time']} [{row['level'].rjust(5, ' ')}] {row['text']}")
    report = "\n".join(report)
    log(f"Logs do DBT:{report}")

    with open("dbt_log.txt", "w+", encoding="utf-8") as log_file:
        log_file.write(report)

    return "dbt_log.txt"

# =============================
# SUMMARIZERS
# =============================


class RunResultSummarizer:
    """
    A class that summarizes the result of a DBT run.

    Methods:
    - summarize(result): Summarizes the result based on its status.
    - error(result): Returns an error message for the given result.
    - fail(result): Returns a fail message for the given result.
    - warn(result): Returns a warning message for the given result.
    """

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result):
        return f"`{result.node.name}`\n  {result.message.replace('__', '_')} \n"

    def fail(self, result):
        return f"`{result.node.name}`\n   {result.message}: ``` select * from {result.node.relation_name.replace('`','')}``` \n"  # noqa

    def warn(self, result):
        return f"`{result.node.name}`\n  {result.message.replace('__', '_')} \n"


class FreshnessResultSummarizer:
    """
    A class that summarizes the freshness result of a DBT node.

    Methods:
    - summarize(result): Summarizes the freshness result based on its status.
    - error(result): Returns the error message for a failed freshness result.
    - fail(result): Returns the name of the failed freshness result.
    - warn(result): Returns the warning message for a stale freshness result.
    """

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result):
        freshness = result.node.freshness
        error_criteria = f">={freshness.error_after.count} {freshness.error_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({error_criteria})"

    def fail(self, result):
        return f"{result.node.relation_name.replace('`', '')}"

    def warn(self, result):
        freshness = result.node.freshness
        warn_criteria = f">={freshness.warn_after.count} {freshness.warn_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({warn_criteria})"


class Summarizer:
    """
    A class that provides summarization functionality for different result types.
    This class can be called with a result object and it will return a summarized version
        of the result.

    Attributes:
        None

    Methods:
        __call__: Returns a summarized version of the given result object.

    """

    def __call__(self, result):
        if isinstance(result, RunResult):
            return RunResultSummarizer().summarize(result)
        elif isinstance(result, SourceFreshnessResult):
            return FreshnessResultSummarizer().summarize(result)
        else:
            raise ValueError(f"Unknown result type: {type(result)}")


# GCP - UTILS


def download_from_cloud_storage(path: str, bucket_name: str, blob_prefix: str = None):
    """
    Downloads files from Google Cloud Storage to the specified local path.

    Args:
        path (str): The local path where the files will be downloaded to.
        bucket_name (str): The name of the Google Cloud Storage bucket.
        blob_prefix (str, optional): The prefix of the blobs to download. Defaults to None.
    """

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    downloaded_files = []

    if not os.path.exists(path):
        os.makedirs(path)

    blobs = bucket.list_blobs(prefix=blob_prefix)

    for blob in blobs:
        destination_file_name = os.path.join(path, blob.name)

        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)

        try:
            blob.download_to_filename(destination_file_name)

            downloaded_files.append(destination_file_name)

        except IsADirectoryError:
            pass

    return downloaded_files


def upload_to_cloud_storage(
    path: str,
    bucket_name: str,
    blob_prefix: str = None,
    if_exist: str = "replace",
):
    """
    Uploads a file or a folder to Google Cloud Storage.

    Args:
        path (str): The path to the file or folder that needs to be uploaded.
        bucket_name (str): The name of the bucket in Google Cloud Storage.
        blob_prefix (str, optional): The prefix of the blob to upload. Defaults to None.
        if_exist (str, optional): The action to take if the blob already exists. Defaults to "replace".
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    if if_exist not in ["replace", "skip"]:
        raise ValueError("Invalid if_exist value. Please use 'replace' or 'skip'.")

    if os.path.isfile(path):
        # Upload a single file
        blob_name = os.path.basename(path)
        if blob_prefix:
            blob_name = f"{blob_prefix}/{blob_name}"
        blob = bucket.blob(blob_name)

        if if_exist == "skip" and blob.exists():
            return

        blob.upload_from_filename(path)
    elif os.path.isdir(path):
        # Upload a folder
        for root, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                blob_name = os.path.relpath(file_path, path)
                if blob_prefix:
                    blob_name = f"{blob_prefix}/{blob_name}"
                blob = bucket.blob(blob_name)

                if if_exist == "skip" and blob.exists():
                    continue

                blob.upload_from_filename(file_path)
    else:
        raise ValueError("Invalid path provided.")