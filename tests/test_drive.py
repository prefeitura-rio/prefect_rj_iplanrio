from typing import final

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from utils.drive import confirm_root, ensure_folder, find_folder, list_files, upload


@final
class FakeRequest:
    """A request of the Drive API that gives an answer, or fails, when it runs."""

    def __init__(self, answer: dict[str, object], error: Exception | None = None) -> None:
        """Store the arguments."""
        self.answer = answer
        self.error = error

    def execute(self) -> dict[str, object]:
        """Run the request."""
        if self.error is not None:
            raise self.error

        return self.answer


@final
class FakeFiles:
    """The `files()` resource of the Drive API, with the calls that it saw."""

    def __init__(self, found: list[dict[str, str]], error: Exception | None = None) -> None:
        """Store the arguments."""
        self.found = found
        self.error = error
        self.calls: list[tuple[str, dict[str, object]]] = []

    def list(self, **arguments: object) -> FakeRequest:
        """Answer a search with the files found."""
        self.calls.append(("list", arguments))

        return FakeRequest({"files": self.found})

    def get(self, **arguments: object) -> FakeRequest:
        """Answer the look-up of one file, or fail."""
        self.calls.append(("get", arguments))

        return FakeRequest({"id": "x"}, self.error)

    def create(self, **arguments: object) -> FakeRequest:
        """Answer the creation of a file with a new id."""
        self.calls.append(("create", arguments))

        return FakeRequest({"id": "new-id"})


@final
class FakeDrive:
    """A Drive client with one `files()` resource."""

    def __init__(self, files: FakeFiles) -> None:
        """Store the arguments."""
        self.resource = files

    def files(self) -> FakeFiles:
        """Return the resource."""
        return self.resource


def make_drive(
    found: list[dict[str, str]] | None = None, error: Exception | None = None
) -> tuple[FakeDrive, FakeFiles]:
    """Build a client and the resource that records its calls."""
    files = FakeFiles(found or [], error)

    return FakeDrive(files), files


def test_a_search_asks_all_drives_and_gives_the_files() -> None:
    """Search the shared drives too, and return the files."""
    drive, files = make_drive([{"id": "1", "name": "a"}])

    found = list_files(drive, "name = 'a'")  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client

    assert found == [{"id": "1", "name": "a"}]
    assert files.calls[0][1]["supportsAllDrives"] is True
    assert files.calls[0][1]["includeItemsFromAllDrives"] is True


@pytest.mark.parametrize(
    ("found", "expected"),
    [
        pytest.param([{"id": "7", "name": "v1"}], "7", id="an_existing_folder"),
        pytest.param([], None, id="no_folder"),
    ],
)
def test_a_folder_is_found_by_name_inside_its_parent(found: list[dict[str, str]], expected: str | None) -> None:
    """Search the name inside the parent, and give the id or nothing."""
    drive, files = make_drive(found)

    assert find_folder(drive, "v1", "root") == expected  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client
    assert "'root' in parents" in str(files.calls[0][1]["q"])


def test_a_quote_in_the_name_of_a_folder_is_escaped() -> None:
    """Escape quotes, so the name cannot end the query."""
    drive, files = make_drive()

    find_folder(drive, "o'neil", "root")  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client

    assert "name = 'o\\'neil'" in str(files.calls[0][1]["q"])


def test_a_missing_folder_is_created_in_the_root() -> None:
    """Create the folder under the root when the search finds none."""
    drive, files = make_drive()

    assert ensure_folder(drive, "root", "v1") == "new-id"  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client
    assert files.calls[-1][0] == "create"
    assert files.calls[-1][1]["body"] == {
        "name": "v1",
        "mimeType": "application/vnd.google-apps.folder",
        "parents": ["root"],
    }


def test_an_existing_folder_is_not_created_again() -> None:
    """Return the id that the search finds, and create nothing."""
    drive, files = make_drive([{"id": "7", "name": "v1"}])

    assert ensure_folder(drive, "root", "v1") == "7"  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client
    assert [call[0] for call in files.calls] == ["list"]


def test_a_root_that_cannot_be_reached_is_reported() -> None:
    """Raise `RuntimeError` when the API refuses the root."""
    error = HttpError(Response({"status": 404}), b"not found")
    drive, _ = make_drive(error=error)

    with pytest.raises(RuntimeError, match="not reachable"):
        confirm_root(drive, "root")  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client


def test_a_file_is_uploaded_to_its_folder() -> None:
    """Send the name and the folder, and ask for all drives."""
    drive, files = make_drive()

    upload(drive, "folder", "a.csv", b"x", "text/csv")  # pyright: ignore[reportArgumentType]  # a fake stands in for the Drive client

    arguments = files.calls[0][1]
    assert arguments["body"] == {"name": "a.csv", "parents": ["folder"]}
    assert arguments["supportsAllDrives"] is True
