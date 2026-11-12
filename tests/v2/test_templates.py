"""The published text: every template renders, and a missing value is an error."""

import jinja2
import pytest

from src.v2.catalog.sidecar import method
from src.v2.templates import TEMPLATES_DIR, render, render_paragraph


def test_every_template_is_known() -> None:
    # A new template needs a test here.
    found = sorted(
        str(p.relative_to(TEMPLATES_DIR)) for p in TEMPLATES_DIR.rglob("*.j2")
    )
    assert found == [
        "lineage.txt.j2",
        "mail/failed.txt.j2",
        "mail/operator.txt.j2",
        "mail/ready.txt.j2",
    ]


def test_ready_mail() -> None:
    body = render(
        "mail/ready.txt.j2",
        name="Dome",
        catalog_url="https://s/c.json",
        assets={"dnbr": "https://s/dnbr.tif", "thumbnail": "https://s/t.jpg"},
        status_url="https://api/v2/severity/1",
    )
    assert body == (
        "The fire severity analysis for Dome is ready.\n\n"
        "Catalog: https://s/c.json\n"
        "dnbr: https://s/dnbr.tif\n"
        "thumbnail: https://s/t.jpg\n"
        "\nStatus: https://api/v2/severity/1\n"
    )


@pytest.mark.parametrize(
    "status_url, expected",
    [
        (
            "https://api/v2/severity/1",
            "The fire severity analysis for Dome failed: E\n\n"
            "Status: https://api/v2/severity/1\n\n"
            "Submitting the same request again runs it again.\n",
        ),
        (
            None,
            "The fire severity analysis for Dome failed: E\n\n"
            "Submitting the same request again runs it again.\n",
        ),
    ],
)
def test_failed_mail(status_url: object, expected: str) -> None:
    assert (
        render("mail/failed.txt.j2", name="Dome", error="E", status_url=status_url)
        == expected
    )


def test_operator_mail_ends_with_the_trace() -> None:
    trace = "Traceback (most recent call last):\nRuntimeError: boom\n"
    body = render(
        "mail/operator.txt.j2",
        job_id="J",
        name="Dome",
        image="img@sha256:0",
        git_sha="abc1234",
        request='{"a":1}',
        trace=trace,
    )
    assert body.startswith(
        "Job J (Dome) failed.\n\nImage: img@sha256:0\nCommit: abc1234\n"
    )
    assert body.endswith('Request:\n{"a":1}\n\n' + trace)


def test_lineage_is_one_paragraph() -> None:
    text = render_paragraph(
        "lineage.txt.j2",
        sensor="sentinel-2",
        provider="Element 84",
        collection="sentinel-2-l2a",
        prefire=["2020-08-01", "2020-08-15"],
        postfire=["2020-08-24", "2020-09-06"],
        bands={"nir": "nir08", "swir": "swir22", "mask": "scl"},
        mask_description="SCL: cloud",
        composite="median",
        resampling="nearest",
        resolution=20.0,
        epsg=32611,
        pipeline_version=2,
    )
    assert "\n" not in text and "  " not in text
    assert "onto a 20 m grid in EPSG:32611" in text
    assert text.endswith("Pipeline version 2.")


def test_a_missing_value_is_an_error_not_a_blank() -> None:
    with pytest.raises(jinja2.UndefinedError):
        render("mail/failed.txt.j2", name="Dome", status_url=None)


def test_method_file() -> None:
    loaded = method()
    assert loaded["scene_selection"] and loaded["scaling"]
    assert all(p["citation"] and p["doi"] for p in loaded["publications"])
