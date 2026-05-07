"""R2 client roundtrip + check_update logic + missing-cred reporting."""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest


def test_smoke_returns_missing_env_when_unset():
    with patch.dict(os.environ, {}, clear=True):
        from importlib import reload
        import config.r2_client as r2
        reload(r2)
        rep = r2.smoke_check()
        assert rep["ok"] is False
        assert "missing_env" in rep
        assert set(rep["missing_env"]) >= {"R2_ACCOUNT_ID", "R2_BUCKET_NAME"}


def test_exists_returns_none_without_credentials():
    with patch.dict(os.environ, {}, clear=True):
        from importlib import reload
        import config.r2_client as r2
        reload(r2)
        assert r2.exists("any/key") is None


def test_upload_download_roundtrip_with_mocked_boto(tmp_path):
    """Use moto to stand up a fake S3 backend, point R2 client at it."""
    pytest.importorskip("moto")
    from moto import mock_aws
    fake_creds = {
        "R2_ACCOUNT_ID": "x", "R2_ACCESS_KEY_ID": "x",
        "R2_SECRET_ACCESS_KEY": "x", "R2_BUCKET_NAME": "test-bucket",
        "R2_ENDPOINT_URL": "https://s3.amazonaws.com",
        # moto needs SOMETHING in the boto credential chain
        "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    with mock_aws(), patch.dict(os.environ, fake_creds, clear=True):
        import boto3
        boto3.client("s3").create_bucket(Bucket="test-bucket")
        from importlib import reload
        import config.r2_client as r2
        reload(r2)
        # Write a parquet, upload, then download
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        local = tmp_path / "test.parquet"
        df.to_parquet(local, index=False)
        rep = r2.upload(local, "raw/test/data.parquet")
        assert rep["size_bytes"] > 0
        info = r2.exists("raw/test/data.parquet")
        assert info is not None and info["size"] > 0
        # Pass an explicit dest path to avoid Windows tempfile rename issues
        df2 = r2.download("raw/test/data.parquet",
                              dest=tmp_path / "download.parquet")
        assert df2.equals(df)


def test_check_update_logic():
    """Mock r2.exists() to test the freshness-gating logic."""
    from collectors._base import _default_check_update
    with patch("config.r2_client.exists", return_value=None):
        rep = _default_check_update(year=2022, source_name="x")
        assert rep["needs_update"] is True
        assert rep["reason"] == "not in R2"
    # Object exists but is "recent" (today)
    from datetime import datetime, timezone
    fresh = {"size": 100,
              "last_modified": datetime.now(timezone.utc).isoformat()}
    with patch("config.r2_client.exists", return_value=fresh):
        rep = _default_check_update(year=2022, source_name="x")
        assert rep["needs_update"] is False
