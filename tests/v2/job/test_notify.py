"""Best-effort SMTP."""

from email.message import EmailMessage
from typing import Any, List, Optional

import pytest

from src.v2.job.notify import Mailer, SmtpSettings

ENV = {"SMTP_USER": "noreply@example.org", "SMTP_PASSWORD": "mailbox-password"}


class FakeSmtp:
    instances: List["FakeSmtp"] = []

    def __init__(
        self, host: str, port: int, timeout: Optional[float] = None, fail: bool = False
    ):
        self.host, self.port, self.timeout = host, port, timeout
        self.started_tls = False
        self.logged_in: Optional[tuple] = None
        self.sent: List[EmailMessage] = []
        self.fail = fail
        FakeSmtp.instances.append(self)

    def __enter__(self) -> "FakeSmtp":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def starttls(self) -> None:
        self.started_tls = True

    def login(self, user: str, password: str) -> None:
        if self.fail:
            raise OSError("connection reset")
        self.logged_in = (user, password)

    def send_message(self, message: EmailMessage) -> None:
        self.sent.append(message)


@pytest.fixture(autouse=True)
def reset() -> None:
    FakeSmtp.instances = []


def test_settings_from_env() -> None:
    settings = SmtpSettings.from_env({**ENV, "OPERATOR_EMAIL": "ops@example.org"})
    assert settings is not None
    assert (settings.host, settings.port) == ("mail.privateemail.com", 465)
    assert settings.sender == "noreply@example.org"
    assert settings.operator == "ops@example.org"
    assert SmtpSettings.from_env({"SMTP_USER": "x"}) is None


def test_send_over_ssl() -> None:
    mailer = Mailer(SmtpSettings.from_env(ENV), smtp_ssl=FakeSmtp, smtp=FakeSmtp)
    assert mailer.send("to@example.org", "Subject", "Body")
    (server,) = FakeSmtp.instances
    assert (server.host, server.port, server.timeout) == (
        "mail.privateemail.com",
        465,
        30,
    )
    assert not server.started_tls
    assert server.logged_in == ("noreply@example.org", "mailbox-password")
    (message,) = server.sent
    assert (message["To"], message["From"], message["Subject"]) == (
        "to@example.org",
        "noreply@example.org",
        "Subject",
    )


def test_send_with_starttls_on_587() -> None:
    mailer = Mailer(
        SmtpSettings.from_env({**ENV, "SMTP_PORT": "587"}),
        smtp_ssl=FakeSmtp,
        smtp=FakeSmtp,
    )
    assert mailer.send("to@example.org", "Subject", "Body")
    assert FakeSmtp.instances[0].started_tls


def test_failures_are_swallowed() -> None:
    def failing(host: str, port: int, timeout: float) -> FakeSmtp:
        return FakeSmtp(host, port, timeout, fail=True)

    mailer = Mailer(SmtpSettings.from_env(ENV), smtp_ssl=failing, smtp=failing)
    assert mailer.send("to@example.org", "Subject", "Body") is False


def test_unconfigured_or_no_recipient_sends_nothing() -> None:
    assert Mailer(None).send("to@example.org", "Subject", "Body") is False
    mailer = Mailer(SmtpSettings.from_env(ENV), smtp_ssl=FakeSmtp, smtp=FakeSmtp)
    assert mailer.send(None, "Subject", "Body") is False
    assert FakeSmtp.instances == []
