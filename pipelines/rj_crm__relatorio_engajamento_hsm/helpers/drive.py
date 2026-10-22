"""Google Drive: the folders of the reports and the upload of their files.

The root folder lives in a shared drive, not in My Drive, so every call passes `supportsAllDrives`.
Without it the API treats an item of a shared drive as if it did not exist, and answers 404 even
when the service account has access.
"""

from datetime import date
from io import BytesIO
from typing import cast

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import DRIVE_FOLDER_MIME
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import report_slug
from utils.utils import load_credentials

type Files = list[dict[str, str]]


def connect() -> Resource:
    """Return a Drive client with the credentials of the environment."""
    return build("drive", "v3", credentials=load_credentials())


def list_files(drive: Resource, query: str) -> Files:
    """Return the id and the name of the files that a Drive query finds, in all drives."""
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]
    answer = api.list(
        q=query, fields="files(id, name)", spaces="drive", supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()

    return cast("Files", answer.get("files", []))


def find_folder(drive: Resource, name: str, parent_id: str) -> str | None:
    """Return the id of the folder with a name inside a parent, or nothing."""
    escaped = name.replace("\\", "\\\\").replace("'", "\\'")
    files = list_files(
        drive,
        f"name = '{escaped}' and mimeType = '{DRIVE_FOLDER_MIME}' and trashed = false and '{parent_id}' in parents",
    )

    return files[0]["id"] if files else None


def confirm_root(drive: Resource, root_id: str) -> None:
    """Check that the root folder exists and the service account can reach it.

    Raise `RuntimeError` if not. The id is used, not a name, so no other folder with that name gets the files.
    """
    try:
        api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]
        api.get(fileId=root_id, fields="id", supportsAllDrives=True).execute()
    except HttpError as error:
        raise RuntimeError(f"The folder {root_id!r} is not reachable: check that it is shared (Editor)") from error


def hsm_folder_id(drive: Resource, root_id: str, hsm_name: str) -> str:
    """Return the id of the subfolder of an HSM, and create it if it does not exist."""
    if folder_id := find_folder(drive, hsm_name, root_id):
        return folder_id

    body = {"name": hsm_name, "mimeType": DRIVE_FOLDER_MIME, "parents": [root_id]}
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]
    folder = api.create(body=body, fields="id", supportsAllDrives=True).execute()
    logger.info("Folder created: %s", hsm_name)

    return str(folder["id"])


def report_exists(drive: Resource, root_id: str, hsm_name: str, reference: date) -> bool:
    """Tell whether the subfolder of the HSM has a file of the report of a day, found by the prefix of its name."""
    confirm_root(drive, root_id)
    folder_id = find_folder(drive, hsm_name, root_id)

    if folder_id is None:
        return False

    prefix = report_slug(hsm_name, reference)

    return bool(list_files(drive, f"name contains '{prefix}' and '{folder_id}' in parents and trashed = false"))


def upload(drive: Resource, folder_id: str, name: str, content: bytes, mime_type: str) -> None:
    """Upload a file to a folder."""
    media = MediaIoBaseUpload(BytesIO(content), mimetype=mime_type, resumable=False)
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]
    api.create(
        body={"name": name, "parents": [folder_id]}, media_body=media, fields="id", supportsAllDrives=True
    ).execute()
    logger.info("File published: %s", name)
