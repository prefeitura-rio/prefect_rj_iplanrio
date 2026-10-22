"""Types of rj_pic__disparos_email."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Mail:
    """A mail to send."""

    to: str
    name: str
    subject: str
    body: str


@dataclass(frozen=True)
class Delivery:
    """The result of the run: the mails that went out and the ones that did not."""

    sent: int
    failed: int
