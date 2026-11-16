"""Our AGENTS.md for the catalog and each collection, from config/agents/.

portolan scaffolds an AGENTS.md of empty prompts wherever one is missing and
never overwrites one that is there (``portolan_cli.agents_md``), so a publisher
fills it in by writing the file before ``portolan init`` and ``add``. The
templates are Markdown with ``$name`` placeholders (``string.Template``); an
unknown placeholder fails the build.
"""

from pathlib import Path

from string import Template

AGENTS_FILE = "AGENTS.md"
TEMPLATES = Path(__file__).resolve().parents[3] / "config" / "agents"
CATALOG = "catalog"
INDICES = "indices"
PERIMETER = "perimeter"
HEX = "hex"


def render_agents(template: str, **values: object) -> str:
    """A template from config/agents/ with its placeholders filled."""
    text = (TEMPLATES / f"{template}.md").read_text()
    return Template(text).substitute({k: str(v) for k, v in values.items()})


def write_agents(folder: Path, template: str, **values: object) -> Path:
    """Write ``folder/AGENTS.md`` from a template; return its path."""
    path = folder / AGENTS_FILE
    path.write_text(render_agents(template, **values))
    return path
