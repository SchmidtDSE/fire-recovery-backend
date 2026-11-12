"""Store settings from the S3_* environment, and how records are written."""

from typing import Any, Dict

import pytest

import src.v2.store as store_module
from src.v2.store import V1_BUCKET, Store, StoreSettings

CREDENTIALS = {"S3_ACCESS_KEY_ID": "key", "S3_SECRET_ACCESS_KEY": "secret"}


def test_records_are_written_uncacheable(monkeypatch: pytest.MonkeyPatch) -> None:
    # GCS serves a cached public object for an hour, so a rerun's record would
    # read back as the previous run's (seen on josh's cluster).
    calls: Dict[str, Any] = {}
    monkeypatch.setattr(store_module.obstore, "put", lambda *a, **kw: calls.update(kw))
    Store(StoreSettings.from_env(CREDENTIALS)).put_json("v2/jobs/x.json", {"a": 1})
    assert calls["attributes"]["Cache-Control"] == "no-store"
    assert calls["attributes"]["Content-Type"] == "application/json"


def test_defaults_are_production_gcs_in_v2s_own_bucket() -> None:
    settings = StoreSettings.from_env(CREDENTIALS)
    assert settings.endpoint_url == "https://storage.googleapis.com"
    assert settings.bucket == "dse-nps-fire-severity-storage"
    assert settings.bucket != V1_BUCKET
    assert settings.region == "auto"
    assert (
        settings.public_base_url
        == "https://storage.googleapis.com/dse-nps-fire-severity-storage"
    )


def test_local_endpoint() -> None:
    settings = StoreSettings.from_env(
        {
            **CREDENTIALS,
            "S3_ENDPOINT": "localhost:9000",
            "S3_SECURE": "False",
            "S3_BUCKET": "test-bucket",
            "S3_REGION": "us-east-1",
        }
    )
    assert settings.endpoint_url == "http://localhost:9000"
    assert settings.public_base_url == "http://localhost:9000/test-bucket"
    assert settings.aws_env() == {
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }


def test_public_base_url_override() -> None:
    settings = StoreSettings.from_env(
        {**CREDENTIALS, "S3_PUBLIC_BASE_URL": "https://data.example.org/fires/"}
    )
    assert settings.public_base_url == "https://data.example.org/fires"
    assert "AWS_ALLOW_HTTP" not in settings.aws_env()


def test_credentials_are_required() -> None:
    with pytest.raises(ValueError, match="S3_ACCESS_KEY_ID"):
        StoreSettings.from_env({"S3_ACCESS_KEY_ID": "key"})
