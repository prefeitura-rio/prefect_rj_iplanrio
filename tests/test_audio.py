from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import JsonValue

from utils.audio import AudioProcessingError, check_audio, find_audio_url, transcribe_attendance


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        pytest.param(
            {"media": {"file": "https://x/a.ogg", "contentType": "audio/ogg"}},
            "https://x/a.ogg",
            id="an_audio_without_text",
        ),
        pytest.param(
            {"media": {"file": "https://x/a.mp3", "contentType": ""}}, "https://x/a.mp3", id="an_audio_by_its_extension"
        ),
        pytest.param(
            {"text": "hi", "media": {"file": "https://x/a.ogg", "contentType": "audio/ogg"}},
            None,
            id="a_message_with_text",
        ),
        pytest.param({"media": {"file": "https://x/a.png", "contentType": "image/png"}}, None, id="an_image"),
        pytest.param({"media": None}, None, id="no_media"),
        pytest.param({}, None, id="a_text_message"),
    ],
)
def test_an_audio_is_a_message_without_text_that_has_an_audio_file(
    message: dict[str, JsonValue], expected: str | None
) -> None:
    """Find the audio that needs a transcription."""
    assert find_audio_url(message) == expected


def test_a_missing_or_empty_file_is_refused(tmp_path: Path) -> None:
    """Refuse a file that is not there or has no bytes."""
    with pytest.raises(FileNotFoundError):
        check_audio(tmp_path / "none.ogg", 300)

    empty = tmp_path / "empty.ogg"
    empty.write_bytes(b"")

    with pytest.raises(AudioProcessingError, match="empty"):
        check_audio(empty, 300)


def test_the_audio_of_an_attendance_is_transcribed_and_a_failure_leaves_the_text_empty() -> None:
    """Fill the text of the audio messages, and only theirs."""
    item: dict[str, JsonValue] = {
        "messages": [
            {"id": 1, "text": "hello"},
            {"id": 2, "media": {"file": "https://x/ok.ogg", "contentType": "audio/ogg"}},
            {"id": 3, "media": {"file": "https://x/bad.ogg", "contentType": "audio/ogg"}},
        ]
    }

    def transcribe(url: str) -> str:
        if "bad" in url:
            raise AudioProcessingError("too long")
        return "transcript"

    before = deepcopy(item)
    result = transcribe_attendance(item, transcribe)

    assert result["messages"] == [
        {"id": 1, "text": "hello"},
        {"id": 2, "media": {"file": "https://x/ok.ogg", "contentType": "audio/ogg"}, "text": "transcript"},
        {"id": 3, "media": {"file": "https://x/bad.ogg", "contentType": "audio/ogg"}, "text": None},
    ]
    assert item == before


def test_an_attendance_without_audio_is_returned_as_it_is() -> None:
    """Leave the attendance alone when it has no audio."""
    item: dict[str, JsonValue] = {"messages": [{"id": 1, "text": "hi"}]}

    assert transcribe_attendance(item, lambda url: url) == item
