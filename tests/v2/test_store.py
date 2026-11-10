"""Store settings from the S3_* environment."""

import pytest

from src.v2.store import StoreSettings

CREDENTIALS = {"S3_ACCESS_KEY_ID": "key", "S3_SECRET_ACCESS_KEY": "secret"}


def test_defaults_are_production_gcs() -> None:
    settings = StoreSettings.from_env(CREDENTIALS)
    assert settings.endpoint_url == "https://storage.googleapis.com"
    assert settings.bucket == "fire-recovery-store"
    assert settings.region == "auto"
    assert (
        settings.public_base_url == "https://storage.googleapis.com/fire-recovery-store"
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
