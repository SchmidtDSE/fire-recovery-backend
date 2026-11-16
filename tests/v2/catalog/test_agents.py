"""The AGENTS.md templates fill in, and name only placeholders the build sets."""

import pytest

from src.v2.catalog import agents

NAMES = {
    "fire_id": "eureka-badc3220",
    "fire_event_name": "Eureka",
    "catalog_url": "https://example.org/eureka-badc3220",
}


@pytest.mark.parametrize(
    "template, extra",
    [
        (agents.CATALOG, {}),
        (agents.INDICES, {}),
        (agents.PERIMETER, {}),
        (agents.HEX, {"levels": "r12, r13", "finest": 13}),
    ],
)
def test_templates_fill_in(template: str, extra: dict) -> None:
    text = agents.render_agents(template, **NAMES, **extra)
    assert "$" not in text
    assert text.startswith("# AGENTS.md — Eureka ")


def test_a_missing_value_fails() -> None:
    with pytest.raises(KeyError):
        agents.render_agents(agents.HEX, **NAMES)
