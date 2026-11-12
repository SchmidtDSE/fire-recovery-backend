"""Publish an analysis's catalog to its own prefix (design §2 step 9, §7).

The prefix belongs to one job id, and a cluster runs at most one Job per id,
so emptying it first can't race another writer. ``portolan push`` then writes
assets, items, collection.json and catalog.json in that order, so a reader
never sees a catalog whose files are missing.
"""

import os
from pathlib import Path

from src.v2.catalog.build import executable, run_tool
from src.v2.store import Store

FIRES_PREFIX = "v2/fires"


def catalog_prefix(fire_id: str) -> str:
    return f"{FIRES_PREFIX}/{fire_id}"


def publish(catalog_root: Path, fire_id: str, store: Store) -> str:
    """Replace whatever is under the analysis's prefix with the built catalog.

    Returns:
        The public URL of the published catalog.json.
    """
    prefix = catalog_prefix(fire_id)
    store.delete_prefix(f"{prefix}/")
    env = {**os.environ, **store.settings.aws_env()}
    run_tool(
        [
            executable("portolan"),
            "push",
            f"s3://{store.settings.bucket}/{prefix}",
            "--catalog",
            str(catalog_root),
        ],
        cwd=catalog_root,
        env=env,
    )
    return store.public_url(f"{prefix}/catalog.json")
