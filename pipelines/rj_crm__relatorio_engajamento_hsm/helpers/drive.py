"""Google Drive: the folder of an HSM and the file of its report of a day."""

from datetime import date

from googleapiclient.discovery import Resource

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.report import report_slug
from utils.drive import confirm_root, find_folder, list_files


def report_exists(drive: Resource, root_id: str, hsm_name: str, reference: date) -> bool:
    """Tell whether the HSM subfolder has a report file of a day, by its name prefix."""
    confirm_root(drive, root_id)
    folder_id = find_folder(drive, hsm_name, root_id)

    if folder_id is None:
        return False

    prefix = report_slug(hsm_name, reference)

    return bool(list_files(drive, f"name contains '{prefix}' and '{folder_id}' in parents and trashed = false"))
