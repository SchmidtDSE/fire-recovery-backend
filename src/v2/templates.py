"""The text the pipeline publishes, from Jinja templates in config/templates/.

The method statement and the email bodies live there as plain files, so they
can be read and edited without touching code. A template that names a value
the code doesn't pass raises (StrictUndefined) rather than rendering a gap.
"""

from functools import lru_cache
from pathlib import Path
from typing import Any

import jinja2

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "config" / "templates"


@lru_cache(maxsize=1)
def environment() -> jinja2.Environment:
    # Plain text, never HTML: nothing to escape. Block tags take their own line
    # with them, so templates read like the text they produce.
    return jinja2.Environment(  # noqa: S701
        loader=jinja2.FileSystemLoader(TEMPLATES_DIR),
        undefined=jinja2.StrictUndefined,
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )


def render(template: str, **values: Any) -> str:
    """Render a template, e.g. ``render("mail/ready.txt.j2", name=...)``."""
    return environment().get_template(template).render(**values)


def render_paragraph(template: str, **values: Any) -> str:
    """Render a template as one paragraph: every run of whitespace becomes a space."""
    return " ".join(render(template, **values).split())
