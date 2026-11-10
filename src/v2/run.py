"""``python -m src.v2.run``: one analysis, start to finish (design §1, §2).

The Job's only input is the canonical request, in SEVERITY_REQUEST; the job id
is derived from it, so the code that computed an id is the code that runs it.
The run searches, composites, builds and validates the catalog, publishes it,
writes the result record and emails the requester.

Nothing here retries. On any exception the job writes a failed record, sends
the requester a short notice and the operator the traceback, and exits 1.
Kubernetes retries only what the platform did (an evicted or preempted pod),
under a deadline, and a failed analysis runs again only when resubmitted.
"""

import argparse
import logging
import os
import resource
import shutil
import sys
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional

from src.v2.catalog import build_catalog
from src.v2.constants import ENV_GIT_SHA, ENV_IMAGE
from src.v2.job_id import CanonicalRequest, fire_id
from src.v2.notify import Mailer, SmtpSettings
from src.v2.pipeline import compute
from src.v2.portolan_plugin import ITEM_ID, THUMBNAIL_FILE
from src.v2.publish import catalog_prefix, publish
from src.v2.record import ResultRecord, record_key
from src.v2.sidecar import build_sidecar
from src.v2.store import Store

logger = logging.getLogger("src.v2.run")

ENV_REQUEST = "SEVERITY_REQUEST"
ENV_JOB_ID = "JOB_ID"
ENV_NOTIFY_EMAIL = "NOTIFY_EMAIL"
ENV_STATUS_URL = "STATUS_URL"
ENV_WORKDIR = "SEVERITY_WORKDIR"
ENV_THREADS = "SEVERITY_THREADS"
# The Job's CPU request; more threads read faster but hold more blocks at once.
DEFAULT_THREADS = 2
ERROR_SUMMARY_CHARS = 500

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_BAD_INPUT = 2


@dataclass(frozen=True)
class RunContext:
    """Everything one run needs besides the store and the mailer."""

    canonical: CanonicalRequest
    job_id: str
    fire_id: str
    notify_email: Optional[str]
    status_url: Optional[str]
    workdir: Path
    threads: int
    git_sha: Optional[str]
    image: Optional[str]


def peak_rss_mib() -> tuple[int, int]:
    """Peak resident memory, in MiB, of this process and of its largest child."""
    own = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss // 1024
    return int(own), int(child)


def summarize(error: BaseException) -> str:
    """A one-line reason for the record and the requester."""
    text = f"{type(error).__name__}: {error}".replace("\n", " ")
    return text[:ERROR_SUMMARY_CHARS]


def analyze(ctx: RunContext, store: Store) -> ResultRecord:
    """Compute, build, validate and publish; return the succeeded record."""
    canonical = ctx.canonical
    result = compute(canonical, ctx.workdir, ctx.threads)
    sidecar = build_sidecar(
        canonical, ctx.job_id, result.grid, result.provenance, result.cogs
    )
    source = result.provenance.source
    catalog = build_catalog(
        ctx.workdir,
        ctx.fire_id,
        result.cogs,
        sidecar,
        title=f"{canonical.fire_event_name} burn severity",
        description=(
            f"dNBR, RdNBR and RBR for {canonical.fire_event_name} from "
            f"{canonical.sensor}: prefire {' to '.join(canonical.prefire_window)}, "
            f"postfire {' to '.join(canonical.postfire_window)}."
        ),
        item_datetime=canonical.postfire_window[1],
        source_url=f"{source.api.rstrip('/')}/collections/{source.collection}",
    )
    catalog_url = publish(catalog.root, ctx.fire_id, store)
    item_prefix = f"{catalog_prefix(ctx.fire_id)}/{ctx.fire_id}/{ITEM_ID}"
    assets = {
        name: store.public_url(f"{item_prefix}/{name}.tif") for name in result.cogs
    }
    assets["thumbnail"] = store.public_url(f"{item_prefix}/{THUMBNAIL_FILE}")
    own, child = peak_rss_mib()
    return ResultRecord(
        job_id=ctx.job_id,
        state="succeeded",
        pipeline_version=canonical.pipeline_version,
        fire_event_name=canonical.fire_event_name,
        fire_id=ctx.fire_id,
        git_sha=ctx.git_sha,
        image=ctx.image,
        catalog_url=catalog_url,
        assets=assets,
        scene_counts={
            "prefire": len(result.provenance.prefire_scenes),
            "postfire": len(result.provenance.postfire_scenes),
        },
        provider=source.provider,
        grid=result.grid,
        rashid_warnings=catalog.warnings,
        peak_rss_mib=own,
        peak_child_rss_mib=child,
    )


def status_line(ctx: RunContext) -> str:
    return f"\nStatus: {ctx.status_url}\n" if ctx.status_url else ""


def notify_success(ctx: RunContext, record: ResultRecord, mailer: Mailer) -> None:
    name = ctx.canonical.fire_event_name
    mailer.send(
        ctx.notify_email,
        f"Fire severity ready: {name}",
        f"The fire severity analysis for {name} is ready.\n\n"
        f"Catalog: {record.catalog_url}\n"
        + "".join(f"{asset}: {url}\n" for asset, url in record.assets.items())
        + status_line(ctx),
    )


def notify_failure(
    ctx: RunContext, error: BaseException, trace: str, mailer: Mailer
) -> None:
    name = ctx.canonical.fire_event_name
    mailer.send(
        ctx.notify_email,
        f"Fire severity failed: {name}",
        f"The fire severity analysis for {name} failed: {summarize(error)}\n"
        f"{status_line(ctx)}\n"
        f"Submitting the same request again runs it again.\n",
    )
    mailer.send(
        mailer.operator,
        f"[severity-v2] job {ctx.job_id} failed",
        f"Job {ctx.job_id} ({name}) failed.\n\n"
        f"Image: {ctx.image}\nCommit: {ctx.git_sha}\n\n"
        f"Request:\n{ctx.canonical.to_json()}\n\n{trace}",
    )


def run(ctx: RunContext, store: Store, mailer: Mailer) -> int:
    """Run once; report the outcome in the record and by email. Returns the exit code."""
    ctx.workdir.mkdir(parents=True, exist_ok=True)
    try:
        record = analyze(ctx, store)
        store.put_json(record_key(ctx.job_id), record.model_dump(mode="json"))
        logger.info(f"Job {ctx.job_id} succeeded: {record.catalog_url}")
        notify_success(ctx, record, mailer)
        return EXIT_OK
    except Exception as error:
        trace = traceback.format_exc()
        logger.error(f"Job {ctx.job_id} failed:\n{trace}")
        own, child = peak_rss_mib()
        failed = ResultRecord(
            job_id=ctx.job_id,
            state="failed",
            pipeline_version=ctx.canonical.pipeline_version,
            fire_event_name=ctx.canonical.fire_event_name,
            fire_id=ctx.fire_id,
            git_sha=ctx.git_sha,
            image=ctx.image,
            peak_rss_mib=own,
            peak_child_rss_mib=child,
            error=summarize(error),
        )
        try:
            store.put_json(record_key(ctx.job_id), failed.model_dump(mode="json"))
        except Exception as write_error:
            logger.error(f"Could not write the failed record: {write_error}")
        notify_failure(ctx, error, trace, mailer)
        return EXIT_FAILED
    finally:
        shutil.rmtree(ctx.workdir, ignore_errors=True)


def main(
    argv: Optional[List[str]] = None,
    store_factory: Callable[[], Store] = Store.from_env,
    mailer_factory: Callable[[], Mailer] = lambda: Mailer(SmtpSettings.from_env()),
) -> int:
    """Read the request from the environment (or --request-file) and run it."""
    parser = argparse.ArgumentParser(prog="python -m src.v2.run")
    parser.add_argument(
        "--request-file",
        type=Path,
        help=f"Canonical request JSON instead of ${ENV_REQUEST}",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    text = (
        args.request_file.read_text()
        if args.request_file
        else os.environ.get(ENV_REQUEST)
    )
    if not text:
        logger.error(f"No request: set {ENV_REQUEST} or pass --request-file")
        return EXIT_BAD_INPUT
    canonical = CanonicalRequest.model_validate_json(text)
    job_id = canonical.job_id()
    expected = os.environ.get(ENV_JOB_ID)
    if expected and expected != job_id:
        # The API and this image derive ids differently: a version skew. Abort
        # before writing anything under either id.
        logger.error(f"Request hashes to {job_id}, not {ENV_JOB_ID}={expected}")
        return EXIT_BAD_INPUT

    base = Path(os.environ.get(ENV_WORKDIR) or tempfile.gettempdir())
    ctx = RunContext(
        canonical=canonical,
        job_id=job_id,
        fire_id=fire_id(canonical.fire_event_name, job_id),
        notify_email=os.environ.get(ENV_NOTIFY_EMAIL) or None,
        status_url=os.environ.get(ENV_STATUS_URL) or None,
        workdir=base / f"severity-{job_id}",
        threads=int(os.environ.get(ENV_THREADS) or DEFAULT_THREADS),
        git_sha=os.environ.get(ENV_GIT_SHA) or None,
        image=os.environ.get(ENV_IMAGE) or None,
    )
    logger.info(f"Job {job_id}: {ctx.fire_id}")
    return run(ctx, store_factory(), mailer_factory())


if __name__ == "__main__":
    sys.exit(main())
