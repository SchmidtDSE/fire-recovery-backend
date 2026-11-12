# Fire Recovery Backend

Instructions for coding agents working in this repository. Format:
[agents.md](https://agents.md).

## Commands

Python / FastAPI, environments managed with [Pixi](https://pixi.sh).
Tasks are declared in `pixi.toml`:

```bash
pixi run pytest         # the test suite
pixi run ruff-check     # lint
pixi run ruff-format    # format
pixi run ruff-fix       # lint, applying fixes
```

Type checking is configured in `mypy.ini`; pytest in `pytest.ini`.

## Context documents (`smrt-design/`)

This repo participates in a cross-repo planning setup. `smrt-design/` holds
two kinds of markdown, both identified by `type:` in their front matter:

- **`type: context`** — explanations of how part of this repo works, written
  for an agent that can read this tree but cannot run it.
- **`type: design`** — plans that span several repos, joined across them by a
  shared `plan` id.

### Filling in a stub

A document with `status: stub` is a request from the planning session: it
needed to know something and could not determine it safely from source alone.
Filling one in is a good task for an agent working **in this repo**, because
it can run the tests and follow the code through a toolchain a higher-level
session does not have.

When you fill one in, set all three:

```yaml
status: current
verified_at: "<the commit sha you checked against>"
covers: ["src/whatever/**"]   # the paths this document describes
```

`verified_at` plus `covers` is what makes staleness a fact rather than an
opinion. Anyone can then run:

```bash
git log <verified_at>..HEAD -- <covers>
```

and know whether the ground has moved. `smrt-context audit` does this across
every repo at once.

### Rules

- **Absence of `source:` means the file is not ours.** `README.md`,
  `docs/`, design notes a person wrote — these are input. Do not rewrite them
  into context documents and do not mark them `source: agent`.
- **One subject per context document.** A document that describes everything
  describes nothing, and long undifferentiated context measurably makes agents
  perform worse than no context at all.
- **Do not restate a generated contract in prose.** If an OpenAPI spec or a
  generated types file already defines an edge, name it and stop. A prose copy
  goes stale silently, in a way `verified_at` cannot catch.
- **Update `verified_at` when you revise.** A document whose content changed
  but whose sha did not is worse than one that is honestly stale.
