"""The bucket, over the S3 protocol, for any S3-compatible endpoint (design §7).

Settings come from the same S3_* variables v1 used: GCS through its S3 endpoint
with HMAC keys in production, RustFS in CI and local development. v2 has its own
bucket, named and provisioned like josh's (``{project}-{app}-storage``), so
nothing v2 does reaches v1's ``fire-recovery-store``.
"""

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

import obstore
from obstore.store import S3Store

DEFAULT_BUCKET = "dse-nps-fire-severity-storage"
# v1's production bucket, which v2 never writes to.
V1_BUCKET = "fire-recovery-store"
DEFAULT_ENDPOINT = "storage.googleapis.com"
# GCS accepts any region in a SigV4 signature; "auto" is its convention.
DEFAULT_REGION = "auto"
JSON_CONTENT_TYPE = "application/json"
# The bucket is public, and GCS caches public objects at its edge for an hour by
# default, authenticated S3 reads included. A record is overwritten when its job
# reruns, and the API decides from what it reads, so records must never be cached.
NO_STORE = "no-store"


@dataclass(frozen=True)
class StoreSettings:
    """Where the bucket is and how to sign for it."""

    endpoint: str  # host[:port], no scheme
    secure: bool
    bucket: str
    access_key: str
    secret_key: str
    region: str
    public_base_url: str

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "StoreSettings":
        """Read S3_ENDPOINT, S3_SECURE, S3_BUCKET, S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY, S3_REGION and S3_PUBLIC_BASE_URL.

        Raises:
            ValueError: if the access key or secret is missing.
        """
        env = os.environ if env is None else env
        access_key = env.get("S3_ACCESS_KEY_ID")
        secret_key = env.get("S3_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            raise ValueError("Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY")
        endpoint = env.get("S3_ENDPOINT") or DEFAULT_ENDPOINT
        secure = (env.get("S3_SECURE") or "True").lower() == "true"
        bucket = env.get("S3_BUCKET") or DEFAULT_BUCKET
        scheme = "https" if secure else "http"
        public = env.get("S3_PUBLIC_BASE_URL") or f"{scheme}://{endpoint}/{bucket}"
        return cls(
            endpoint=endpoint,
            secure=secure,
            bucket=bucket,
            access_key=access_key,
            secret_key=secret_key,
            region=env.get("S3_REGION") or DEFAULT_REGION,
            public_base_url=public.rstrip("/"),
        )

    @property
    def endpoint_url(self) -> str:
        return f"{'https' if self.secure else 'http'}://{self.endpoint}"

    def aws_env(self) -> Dict[str, str]:
        """The same settings as AWS_* variables, for tools that read those."""
        env = {
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
            "AWS_REGION": self.region,
        }
        if not self.secure:
            env["AWS_ALLOW_HTTP"] = "true"
        return env


class Store:
    """JSON objects and prefixes in the bucket."""

    def __init__(self, settings: StoreSettings):
        self.settings = settings
        self._store = S3Store(
            settings.bucket,
            endpoint=settings.endpoint_url,
            access_key_id=settings.access_key,
            secret_access_key=settings.secret_key,
            region=settings.region,
            virtual_hosted_style_request=False,
            client_options={"allow_http": True} if not settings.secure else {},
        )

    @classmethod
    def from_env(cls) -> "Store":
        return cls(StoreSettings.from_env())

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """The object at ``key`` parsed as JSON, or None if there is none."""
        try:
            data = obstore.get(self._store, key).bytes()
        except FileNotFoundError:
            return None
        value: Dict[str, Any] = json.loads(bytes(data))
        return value

    def put_json(self, key: str, value: Dict[str, Any]) -> None:
        """Write ``value`` as JSON at ``key`` in one PUT, never to be cached."""
        obstore.put(
            self._store,
            key,
            json.dumps(value, indent=1).encode(),
            use_multipart=False,
            attributes={"Content-Type": JSON_CONTENT_TYPE, "Cache-Control": NO_STORE},
        )

    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under ``prefix``; return how many there were."""
        keys = [
            meta["path"]
            for batch in obstore.list(self._store, prefix=prefix)
            for meta in batch
        ]
        if keys:
            obstore.delete(self._store, keys)
        return len(keys)

    def public_url(self, key: str) -> str:
        return f"{self.settings.public_base_url}/{key}"
