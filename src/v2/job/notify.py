"""Best-effort email over SMTP (design §8).

Any SMTP mailbox works, over SSL on 465 or STARTTLS on 587, through the standard
library. The deployments use a Namecheap Private Email mailbox on the project's
domain. A failed send is logged and never fails the job; the result is on the
status URL either way. Only Jobs send mail: the API holds the credentials only
to pass them on to each Job's Secret.
"""

import html
import logging
import os
import re
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

logger = logging.getLogger(__name__)

DEFAULT_SMTP_HOST = "mail.privateemail.com"
SMTP_SSL_PORT = 465
SMTP_TIMEOUT_SECONDS = 30
URL = re.compile(r"https?://[^\s<>\"]+")


@dataclass(frozen=True)
class SmtpSettings:
    """Where and as whom to send. The operator gets failure tracebacks."""

    host: str
    port: int
    user: str
    password: str
    sender: str
    operator: Optional[str]

    @classmethod
    def from_env(
        cls, env: Optional[Mapping[str, str]] = None
    ) -> Optional["SmtpSettings"]:
        """SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, OPERATOR_EMAIL.

        None when the account isn't configured: mail is then skipped.
        """
        env = os.environ if env is None else env
        user, password = env.get("SMTP_USER"), env.get("SMTP_PASSWORD")
        if not user or not password:
            return None
        return cls(
            host=env.get("SMTP_HOST") or DEFAULT_SMTP_HOST,
            port=int(env.get("SMTP_PORT") or SMTP_SSL_PORT),
            user=user,
            password=password,
            sender=env.get("SMTP_FROM") or user,
            operator=env.get("OPERATOR_EMAIL") or None,
        )


def html_body(text: str, image_cid: str) -> str:
    """The plain text as HTML, its links clickable, with the image after the
    first paragraph."""

    def block(part: str) -> str:
        linked = URL.sub(
            lambda m: f'<a href="{m.group(0)}">{m.group(0)}</a>', html.escape(part)
        )
        return f'<div style="white-space: pre-wrap">{linked}</div>'

    first, _, rest = text.partition("\n\n")
    image = (
        f'<p><img src="cid:{image_cid}" alt="Burn severity thumbnail" '
        'style="max-width: 100%"></p>'
    )
    return block(first) + image + (block(rest) if rest else "")


def attach_inline_image(message: EmailMessage, image: Path) -> None:
    """Add an HTML alternative showing ``image``. The plain text stays as is.
    An unreadable image is logged and left out."""
    try:
        data = image.read_bytes()
    except OSError as e:
        logger.warning(f"Sending without the image {image}: {e}")
        return
    cid = make_msgid(domain="fire-severity")
    message.add_alternative(html_body(message.get_content(), cid[1:-1]), subtype="html")
    html_part = message.get_body(preferencelist=("html",))
    if html_part is None:  # add_alternative just made it
        raise RuntimeError("no HTML part to hold the image")
    html_part.add_related(data, maintype="image", subtype="jpeg", cid=cid)


# Called as factory(host, port, timeout=...), like smtplib.SMTP and SMTP_SSL.
SmtpFactory = Callable[..., Any]


class Mailer:
    """Sends plain-text messages; returns whether each send succeeded."""

    def __init__(
        self,
        settings: Optional[SmtpSettings],
        smtp_ssl: SmtpFactory = smtplib.SMTP_SSL,
        smtp: SmtpFactory = smtplib.SMTP,
    ):
        self.settings = settings
        self._smtp_ssl = smtp_ssl
        self._smtp = smtp

    @property
    def operator(self) -> Optional[str]:
        return self.settings.operator if self.settings else None

    def send(
        self, to: Optional[str], subject: str, body: str, image: Optional[Path] = None
    ) -> bool:
        """Send one message, with ``image`` (a JPEG) shown inline if given. Never
        raises."""
        if not to:
            return False
        if self.settings is None:
            logger.info(f"Mail not configured; not sending {subject!r}")
            return False
        message = EmailMessage()
        message["From"] = self.settings.sender
        message["To"] = to
        message["Subject"] = subject
        message.set_content(body)
        if image is not None:
            try:
                attach_inline_image(message, image)
            except Exception as e:  # the picture is a nicety; the links matter
                logger.warning(f"Sending {subject!r} without its image: {e}")
        try:
            s = self.settings
            if s.port == SMTP_SSL_PORT:
                server = self._smtp_ssl(s.host, s.port, timeout=SMTP_TIMEOUT_SECONDS)
            else:
                server = self._smtp(s.host, s.port, timeout=SMTP_TIMEOUT_SECONDS)
                server.starttls()
            with server:
                server.login(s.user, s.password)
                server.send_message(message)
            return True
        except Exception as e:
            logger.warning(f"Could not send {subject!r}: {e}")
            return False
