from pathlib import Path
from types import SimpleNamespace

import pytest

from src.io import gcs


def test_parse_gcs_uri_happy_path() -> None:
    bucket, blob = gcs.parse_gcs_uri("gs://my-bucket/path/to/object.txt")
    assert bucket == "my-bucket"
    assert blob == "path/to/object.txt"


def test_parse_gcs_uri_bucket_only() -> None:
    bucket, blob = gcs.parse_gcs_uri("gs://my-bucket")
    assert bucket == "my-bucket"
    assert blob == ""


def test_parse_gcs_uri_rejects_non_gcs() -> None:
    with pytest.raises(ValueError):
        gcs.parse_gcs_uri("https://example.com/foo")


def test_sync_local_to_gcs_uses_client_and_bucket(tmp_path: Path) -> None:
    local = tmp_path / "file.txt"
    local.write_text("hello")

    uploaded = {}

    def fake_upload_from_filename(filename: str) -> None:
        uploaded["filename"] = filename

    fake_blob = SimpleNamespace(upload_from_filename=fake_upload_from_filename)

    def fake_blob_factory(name: str):
        uploaded["blob_name"] = name
        return fake_blob

    fake_bucket = SimpleNamespace(blob=fake_blob_factory)
    fake_client = SimpleNamespace(bucket=lambda name: fake_bucket)

    result_uri = gcs.sync_local_to_gcs(local, "gs://test-bucket/path/in/bucket.txt", client=fake_client)  # type: ignore[arg-type]

    assert result_uri == "gs://test-bucket/path/in/bucket.txt"
    assert uploaded["filename"] == str(local)
    assert uploaded["blob_name"] == "path/in/bucket.txt"


def test_sync_local_to_gcs_infers_blob_name_from_filename(tmp_path: Path) -> None:
    local = tmp_path / "another.txt"
    local.write_text("world")

    uploaded = {}

    def fake_upload_from_filename(filename: str) -> None:
        uploaded["filename"] = filename

    fake_blob = SimpleNamespace(upload_from_filename=fake_upload_from_filename)

    def fake_blob_factory(name: str):
        uploaded["blob_name"] = name
        return fake_blob

    fake_bucket = SimpleNamespace(blob=fake_blob_factory)
    fake_client = SimpleNamespace(bucket=lambda name: fake_bucket)

    result_uri = gcs.sync_local_to_gcs(local, "gs://test-bucket", client=fake_client)  # type: ignore[arg-type]

    assert result_uri == "gs://test-bucket/another.txt"
    assert uploaded["filename"] == str(local)
    assert uploaded["blob_name"] == "another.txt"


def test_sync_local_to_gcs_raises_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.txt"
    with pytest.raises(FileNotFoundError):
        gcs.sync_local_to_gcs(missing, "gs://bucket/blob.txt")

