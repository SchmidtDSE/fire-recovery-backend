"""Build one analysis's self-contained Portolan catalog and validate it.

Layout (design §4)::

    {fire_id}/                   catalog.json, README.md, AGENTS.md
      {fire_id}/                 collection.json, README.md, AGENTS.md, items.parquet,
                                 thumbnail.jpg (a copy of RBR's)
        dnbr/                    dnbr.json, dnbr.tif, dnbr.thumb.jpg
        rdnbr/                   rdnbr.json, rdnbr.tif, rdnbr.thumb.jpg
        rbr/                     rbr.json, rbr.tif, rbr.thumb.jpg

One item per index: Portolan makes each distinct data file its own item, and
each index gets its own title, thumbnail and band table.

portolan-cli builds it (``init``, ``add``, ``check --fix``, ``readme``) with the
enrich plugin copying the sidecar into the STAC, and rashid validates it. Any
rashid error fails the build (design §2, step 8).
"""

import json
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import yaml

from src.v2.catalog.portolan_plugin import (
    COLLECTION_THUMBNAIL_FILE,
    COLLECTION_THUMBNAIL_INDEX,
    INDEX_BY_NAME,
    thumbnail_file,
)
from src.v2.catalog.sidecar import SIDECAR_ENV, write_sidecar
from src.v2.catalog.thumbnail import render_thumbnail

logger = logging.getLogger(__name__)

LICENSE = "CC-BY-4.0"
METADATA_TEMPLATE = (
    Path(__file__).resolve().parents[3] / "config" / "portolan_metadata.yaml"
)
# How much of a failing tool's output to keep in the error.
OUTPUT_TAIL_CHARS = 4000


class CatalogError(RuntimeError):
    """A portolan step failed, or rashid found errors in the catalog."""


@dataclass(frozen=True)
class CatalogResult:
    """The built catalog and what rashid reported."""

    root: Path
    warnings: int
    findings: List[Dict[str, Any]] = field(default_factory=list)


def executable(name: str) -> str:
    """A console script from this interpreter's environment, else from PATH."""
    beside = Path(sys.executable).parent / name
    return str(beside) if beside.exists() else (shutil.which(name) or name)


def run_tool(args: Sequence[str], cwd: Path, env: Mapping[str, str]) -> str:
    """Run a CLI step; raise CatalogError with the tail of its output on failure."""
    logger.info(f"Running {' '.join(args[:2])} in {cwd}")
    result = subprocess.run(
        list(args), cwd=cwd, env=dict(env), capture_output=True, text=True
    )
    if result.returncode != 0:
        output = (result.stderr or "") + (result.stdout or "")
        raise CatalogError(
            f"{Path(args[0]).name} {args[1]} exited {result.returncode}: "
            f"{output[-OUTPUT_TAIL_CHARS:]}"
        )
    return result.stdout


def write_metadata(
    root: Path, source_url: Optional[str], processing_notes: Optional[str] = None
) -> None:
    """.portolan/metadata.yaml: contact, license, providers, source and notes.

    ``processing_notes`` is the item's processing:lineage, so the README states
    the same method as the STAC.
    """
    metadata = yaml.safe_load(METADATA_TEMPLATE.read_text())
    metadata["source_url"] = source_url or ""
    if processing_notes:
        metadata["processing_notes"] = processing_notes
    (root / ".portolan" / "metadata.yaml").write_text(
        yaml.safe_dump(metadata, sort_keys=False)
    )


def write_collection_metadata(collection: Path, title: str, description: str) -> None:
    """The collection's own .portolan/metadata.yaml: its title and description.

    portolan takes a collection's title and description only from the
    collection's own file, never from the catalog's (portolan #755).
    """
    (collection / ".portolan").mkdir(parents=True, exist_ok=True)
    (collection / ".portolan" / "metadata.yaml").write_text(
        yaml.safe_dump({"title": title, "description": description}, sort_keys=False)
    )


def validate(root: Path) -> CatalogResult:
    """Run rashid over the whole catalog, bytes included.

    Raises:
        CatalogError: if rashid reports any error or its report can't be read.
    """
    args = [executable("rashid"), "check", str(root), "--schema", "--json", "--all"]
    result = subprocess.run(args, capture_output=True, text=True)
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise CatalogError(
            f"rashid produced no report (exit {result.returncode}): "
            f"{(result.stderr or result.stdout)[-OUTPUT_TAIL_CHARS:]}"
        )
    findings: List[Dict[str, Any]] = report.get("findings", [])
    if report.get("error_count", 0):
        errors = [f for f in findings if f.get("severity") == "error"]
        raise CatalogError(
            f"rashid found {report['error_count']} error(s): "
            f"{json.dumps(errors[:5])[:OUTPUT_TAIL_CHARS]}"
        )
    return CatalogResult(
        root=root, warnings=int(report.get("warning_count", 0)), findings=findings
    )


def item_directory(workdir: Path, fire_id: str, index: str) -> Path:
    """Where an index's item, its COG and its thumbnail, sits in the built catalog."""
    return workdir / "catalog" / fire_id / fire_id / index


def build_catalog(
    workdir: Path,
    fire_id: str,
    cogs: Dict[str, Path],
    sidecar: Dict[str, Any],
    title: str,
    description: str,
    item_datetime: str,
    source_url: Optional[str],
) -> CatalogResult:
    """Lay out the COGs, build the catalog with portolan, validate it with rashid.

    The COGs are moved into the catalog. The sidecar is written outside it, so
    push never uploads it.
    """
    root = workdir / "catalog" / fire_id
    for name, cog in cogs.items():
        item_dir = item_directory(workdir, fire_id, name)
        item_dir.mkdir(parents=True)
        shutil.move(str(cog), item_dir / f"{name}.tif")
        render_thumbnail(
            item_dir / f"{name}.tif",
            item_dir / thumbnail_file(name),
            INDEX_BY_NAME[name].display_range,
        )
    shutil.copyfile(
        item_directory(workdir, fire_id, COLLECTION_THUMBNAIL_INDEX)
        / thumbnail_file(COLLECTION_THUMBNAIL_INDEX),
        root / fire_id / COLLECTION_THUMBNAIL_FILE,
    )
    sidecar_path = write_sidecar(sidecar, workdir / "sidecar.json")

    env = {**os.environ, SIDECAR_ENV: str(sidecar_path)}
    portolan = executable("portolan")
    run_tool(
        [
            portolan, "init", "--auto", "--license", LICENSE, "--id", fire_id,
            "--title", title, "--description", description,
        ],
        cwd=root,
        env=env,
    )  # fmt: skip
    write_metadata(root, source_url, sidecar["properties"].get("processing:lineage"))
    write_collection_metadata(root / fire_id, title, description)
    run_tool([portolan, "add", f"{fire_id}/", "--datetime", item_datetime], root, env)
    run_tool([portolan, "check", "--fix"], root, env)
    run_tool([portolan, "readme"], root, env)
    result = validate(root)
    logger.info(f"Catalog {fire_id} passed rashid with {result.warnings} warning(s)")
    return result
