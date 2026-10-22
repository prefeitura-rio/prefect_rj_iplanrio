"""Download and transcribe the audio of a message."""

from collections.abc import Callable, Mapping
from copy import deepcopy
from pathlib import Path
from tempfile import NamedTemporaryFile

from google.cloud import speech
from httpx2 import Client
from mutagen.mp3 import MP3
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from pydantic import JsonValue
from toolkit.telemetry import logger

from .constants import AUDIO_EXTENSIONS, NO_SPEECH

OGG_EXTENSIONS = ("ogg", "oga", "opus")
DEFAULT_SAMPLE_RATE = 16000


class AudioDownloadError(IOError):
    """The download of an audio file failed."""


class AudioProcessingError(ValueError):
    """An audio file is empty, or longer than the limit."""


class AudioTranscriptionError(Exception):
    """The speech-to-text service failed on an audio file."""


def find_audio_url(message: Mapping[str, JsonValue]) -> str | None:
    """Return the URL of the audio of a message that has no text, or nothing."""
    media = message.get("media")
    url = media.get("file") if isinstance(media, dict) else None

    if message.get("text") or not isinstance(url, str) or not url:
        return None

    content_type = str(media.get("contentType") or "").lower() if isinstance(media, dict) else ""

    return (
        url if "audio" in content_type or url.lower().endswith(tuple(f".{ext}" for ext in AUDIO_EXTENSIONS)) else None
    )


def download_audio(url: str) -> Path:
    """Download an audio file to a temporary path, and return the path.

    Raise `ValueError` for an extension that is not audio, and `AudioDownloadError` if the
    download fails. The caller deletes the file.
    """
    extension = url.lower().rsplit(".", maxsplit=1)[-1]

    if extension not in AUDIO_EXTENSIONS:
        raise ValueError(f"The URL has no supported audio extension: {url}")

    with NamedTemporaryFile(delete=False, suffix=f".{extension}") as file:
        path = Path(file.name)

    try:
        with Client(timeout=60, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            path.write_bytes(response.content)
    except Exception as error:
        path.unlink(missing_ok=True)
        raise AudioDownloadError(f"The download of the audio failed: {type(error).__name__}") from error

    return path


def read_length(info: object) -> float | None:
    """Return the length in seconds of the `info` of a mutagen file, or nothing if it has none."""
    length: object = getattr(info, "length", None)

    return float(length) if isinstance(length, int | float) else None


def read_duration(path: Path) -> float | None:
    """Return the length of an audio file in seconds, or nothing if it cannot be read."""
    extension = path.suffix.lstrip(".").lower()

    try:
        if extension == "mp3":
            return read_length(MP3(path).info)
        if extension == "wav":
            return read_length(WAVE(path).info)
        if extension in OGG_EXTENSIONS:
            try:
                return read_length(OggOpus(path).info)
            except Exception:
                return read_length(OggVorbis(path).info)
    except Exception as error:
        logger.warning("Audio duration unknown: extension=%s error=%s", extension, type(error).__name__)

    return None


def check_audio(path: Path, max_duration_seconds: int) -> None:
    """Check that an audio file exists, has bytes and is not longer than the limit.

    Raise `FileNotFoundError` for a missing file and `AudioProcessingError` for an empty
    or a too long one. A length that cannot be read passes, and the speech service checks it.
    """
    if not path.exists():
        raise FileNotFoundError(f"Audio file not found: {path}")

    if path.stat().st_size == 0:
        raise AudioProcessingError(f"The audio file is empty: {path}")

    duration = read_duration(path)

    if duration is not None and duration > max_duration_seconds:
        raise AudioProcessingError(f"The audio ({duration:.1f}s) is longer than {max_duration_seconds}s")


def transcribe_audio(path: Path, language_code: str = "pt-BR") -> str:
    """Transcribe an audio file with Google Speech-to-Text.

    Raise `AudioTranscriptionError` if the service fails.
    """
    encoding = speech.RecognitionConfig.AudioEncoding.ENCODING_UNSPECIFIED
    sample_rate = DEFAULT_SAMPLE_RATE

    if path.suffix.lstrip(".").lower() in OGG_EXTENSIONS:
        try:
            sample_rate = int(getattr(OggOpus(path).info, "sample_rate", DEFAULT_SAMPLE_RATE))
            encoding = speech.RecognitionConfig.AudioEncoding.OGG_OPUS
        except Exception:
            logger.debug("The audio is not Opus, so the service detects the encoding")

    config = speech.RecognitionConfig(
        encoding=encoding,
        sample_rate_hertz=sample_rate,
        language_code=language_code,
        enable_automatic_punctuation=True,
    )

    try:
        response = speech.SpeechClient().recognize(
            config=config, audio=speech.RecognitionAudio(content=path.read_bytes())
        )
    except Exception as error:
        raise AudioTranscriptionError(f"The transcription failed: {type(error).__name__}") from error

    if not response.results:
        return NO_SPEECH

    return " ".join(result.alternatives[0].transcript for result in response.results).strip()


def transcribe_url(url: str, max_duration_seconds: int) -> str:
    """Download, check and transcribe the audio at a URL, and delete the file."""
    path = download_audio(url)

    try:
        check_audio(path, max_duration_seconds)

        return transcribe_audio(path)
    finally:
        path.unlink(missing_ok=True)


def transcribe_attendance(item: Mapping[str, JsonValue], transcribe: Callable[[str], str]) -> dict[str, JsonValue]:
    """Return a copy of an attendance with the text of each audio message filled.

    `transcribe` turns the URL of an audio into its text. A message whose audio fails keeps
    an empty text, and the failure goes to the log. The attendance is not changed.
    """
    result = deepcopy(dict(item))
    messages = result.get("messages")

    if not isinstance(messages, list):
        return result

    for message in messages:
        if not isinstance(message, dict):
            continue

        url = find_audio_url(message)

        if url is None:
            continue

        try:
            message["text"] = transcribe(url)
        except (
            AudioDownloadError,
            AudioProcessingError,
            AudioTranscriptionError,
            ValueError,
            FileNotFoundError,
        ) as error:
            logger.error("Audio transcription failed: message=%s error=%s", message.get("id"), type(error).__name__)
            message["text"] = None

    return result
