from __future__ import annotations

from pathlib import Path
from typing import Protocol

from google.cloud import storage


class _PathLike(Protocol):
    def __fspath__(self) -> str:  # pragma: no cover - structural protocol
        ...


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """
    Parse a `gs://bucket/path/to/blob` URI into (bucket, blob_name).
    """
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {uri}")
    without_scheme = uri[len("gs://") :]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    blob_name = parts[1] if len(parts) == 2 else ""
    if not bucket:
        raise ValueError(f"Missing bucket in GCS URI: {uri}")
    return bucket, blob_name


def get_client() -> storage.Client:
    """
    Construct a storage Client; kept in a tiny wrapper for easier mocking.
    """
    return storage.Client()


def sync_local_to_gcs(
    local_path: _PathLike,
    gcs_uri: str,
    client: storage.Client | None = None,
) -> str:
    """
    Upload a local file to a GCS URI.

    Returns the final object URI.
    """
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Local file does not exist: {path}")

    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    if not blob_name:
        # If only a bucket is provided, use the filename as the object name.
        blob_name = path.name

    client = client or get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(path))

    return f"gs://{bucket_name}/{blob_name}"

