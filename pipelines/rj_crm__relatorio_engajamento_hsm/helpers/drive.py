"""Google Drive: the folders of the campaigns and the files of the reports."""

from io import BytesIO

from googleapiclient.discovery import Resource
from googleapiclient.http import MediaIoBaseUpload
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    CATALOG_FILE,
    DOCX_MIME,
    DRIVE_DOC_MIME,
    DRIVE_FILE_FIELDS,
    DRIVE_SHEET_MIME,
    QUICK_FOLDER_SUFFIX,
    REVIEWED_MARK,
    TYPE_VALIDATION,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import skipped_status
from pipelines.rj_crm__relatorio_engajamento_hsm.types import DriveFile, Job, Outcome, Prepared
from utils.constants import DRIVE_FOLDER_MIME
from utils.drive import confirm_root, ensure_folder, find_folder


def create_file(drive: Resource, folder_id: str, name: str, mime_type: str) -> DriveFile:
    """Create an empty Google file in a folder and return its id, name and link."""
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]  # the Drive client builds its methods at run time

    return api.create(
        body={"name": name, "mimeType": mime_type, "parents": [folder_id]},
        fields=DRIVE_FILE_FIELDS,
        supportsAllDrives=True,
    ).execute()


def folder_link(folder_id: str) -> str:
    """Return the web link of a Drive folder."""
    return f"https://drive.google.com/drive/folders/{folder_id}"


def upload_doc(drive: Resource, folder_id: str, name: str, document: bytes) -> DriveFile:
    """Upload a Word file that Drive turns into a Google Doc, and return its link."""
    media = MediaIoBaseUpload(BytesIO(document), mimetype=DOCX_MIME, resumable=False)
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]  # the Drive client builds its methods at run time

    return api.create(
        body={"name": name, "mimeType": DRIVE_DOC_MIME, "parents": [folder_id]},
        media_body=media,
        fields=DRIVE_FILE_FIELDS,
        supportsAllDrives=True,
    ).execute()


def escape_query(value: str) -> str:
    """Escape a value for single quotes in a Drive query."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def list_all(drive: Resource, query: str) -> list[DriveFile]:
    """Return all the files of a Drive query, in all drives, and follow every page."""
    api = drive.files()  # pyright: ignore[reportAttributeAccessIssue]  # the Drive client builds its methods at run time
    files: list[DriveFile] = []
    token: str | None = None

    while True:
        answer = api.list(
            q=query,
            fields=f"nextPageToken, files({DRIVE_FILE_FIELDS})",
            spaces="drive",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=token,
        ).execute()
        files += answer.get("files", [])
        token = answer.get("nextPageToken")

        if not token:
            return files


def find_file(drive: Resource, name: str, folder_id: str, mime_type: str) -> DriveFile | None:
    """Return the file with this exact name in a folder, or nothing.

    The name is exact, so `Catálogo_old` never counts as `Catálogo`. A second file with
    the same name logs a warning, and the first one is used.
    """
    query = (
        f"name = '{escape_query(name)}' and '{folder_id}' in parents and trashed = false and mimeType = '{mime_type}'"
    )
    files = list_all(drive, query)

    if len(files) > 1:
        logger.warning("Many files have the same name in a folder: name=%s count=%d", name, len(files))

    return files[0] if files else None


def find_reviewed_sheets(drive: Resource, root_id: str) -> list[DriveFile]:
    """Return the reviewed sheets below a campaign folder, at any depth.

    Drive has no recursive search, so this walks the folders breadth first. It never
    enters a quick report folder, because its files must not feed the example store.
    """
    name_filter = f"name contains '{REVIEWED_MARK}' and trashed = false and mimeType = '{DRIVE_SHEET_MIME}'"
    pending = [root_id]
    found: list[DriveFile] = []

    while pending:
        folder_id = pending.pop(0)
        found += list_all(drive, f"{name_filter} and '{folder_id}' in parents")
        folders = list_all(drive, f"mimeType = '{DRIVE_FOLDER_MIME}' and '{folder_id}' in parents and trashed = false")
        pending += [folder["id"] for folder in folders if not folder["name"].endswith(QUICK_FOLDER_SUFFIX)]

    return found


def prepare(drive: Resource, job: Job, root_id: str, round_name: str) -> Prepared:
    """Make the folder of the campaign and look at what it already has.

    A report whose round folder exists ends here as skipped. A validation never looks at
    a round, and its `Catalog` check is the business of the caller.
    """
    confirm_root(drive, root_id)
    folder_id = ensure_folder(drive, root_id, job.campaign)
    catalog = find_file(drive, CATALOG_FILE, folder_id, DRIVE_SHEET_MIME)
    catalog_id = catalog["id"] if catalog else ""
    catalog_link = catalog.get("webViewLink", "") if catalog else ""

    if job.request_type != TYPE_VALIDATION and (round_id := find_folder(drive, round_name, folder_id)):
        outcome = Outcome(skipped_status(f"relatório '{round_name}' já existe"), folder_link(round_id))

        return Prepared(folder_id, outcome, catalog_id, catalog_link)

    return Prepared(folder_id, None, catalog_id, catalog_link)
