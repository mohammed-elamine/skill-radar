"""Thin S3 abstraction over boto3, compatible with MinIO.

Credentials are resolved from the standard boto3 chain
(``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` environment variables).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, cast

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)

from .exceptions import ObjectAlreadyExistsError, ObjectNotFoundError, UploadError

if TYPE_CHECKING:
    from pathlib import Path

    from mypy_boto3_s3 import S3Client as BotoS3Client

    from skill_radar.config.models import PlatformSettings, S3Config

logger = logging.getLogger(__name__)


class S3Client:
    """Gateway for all S3 / MinIO object-storage operations.

    Parameters
    ----------
    config:
        Platform configuration providing bucket name and region.
    endpoint_url:
        S3/MinIO endpoint URL. Use runtime context to resolve the appropriate
        endpoint (e.g., via resolve_s3_endpoint(s3_config, ctx)).
    """

    def __init__(self, config: PlatformSettings, endpoint_url: str) -> None:
        s3_cfg: S3Config = config.storage.s3
        self._bucket: str = s3_cfg.bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=s3_cfg.region,
            use_ssl=s3_cfg.secure,
        )

    @property
    def bucket(self) -> str:
        """Configured bucket name."""
        return self._bucket

    # -- queries ---------------------------------------------------------

    def object_exists(self, key: str) -> bool:
        """Return ``True`` if *key* exists in the configured bucket."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise
        return True

    def get_object_checksum(self, key: str) -> str:
        """Return the ETag (MD5 for single-part uploads) of an existing object."""
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                raise ObjectNotFoundError(f"Object not found: {key}") from exc
            raise

        etag = cast("str", resp.get("ETag"))
        if not etag:
            raise RuntimeError(f"Missing ETag for object: {key}")
        return etag.strip('"')

    # -- mutations -------------------------------------------------------

    def upload_file(
        self,
        local_path: Path,
        key: str,
        *,
        overwrite: bool = False,
    ) -> None:
        """Upload a local file to S3.

        Raises
        ------
        ObjectAlreadyExistsError
            If *overwrite* is ``False`` and the object already exists.
        UploadError
            On any other upload failure.
        """
        if not overwrite and self.object_exists(key):
            raise ObjectAlreadyExistsError(f"Object already exists: s3://{self._bucket}/{key}")
        try:
            self._client.upload_file(str(local_path), self._bucket, key)
            logger.info("Uploaded %s → s3://%s/%s", local_path.name, self._bucket, key)
        except ClientError as exc:
            raise UploadError(f"Upload failed for {key}: {exc}") from exc

    def upload_bytes(
        self,
        data: bytes,
        key: str,
        *,
        overwrite: bool = False,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload raw bytes to S3.

        Raises
        ------
        ObjectAlreadyExistsError
            If *overwrite* is ``False`` and the object already exists.
        UploadError
            On any other upload failure.
        """
        if not overwrite and self.object_exists(key):
            raise ObjectAlreadyExistsError(f"Object already exists: s3://{self._bucket}/{key}")
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
            logger.info("Uploaded bytes → s3://%s/%s", self._bucket, key)
        except ClientError as exc:
            raise UploadError(f"Upload failed for {key}: {exc}") from exc


# Timeout configurations for different purposes
_HEALTHCHECK_TIMEOUTS = BotoConfig(
    connect_timeout=2,
    read_timeout=3,
    retries={"max_attempts": 1, "mode": "standard"},
)

_DEFAULT_TIMEOUTS = BotoConfig(
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 3, "mode": "adaptive"},
)


def build_s3_client(
    s3_config: S3Config,
    endpoint_url: str,
    *,
    purpose: Literal["default", "healthcheck"] = "default",
) -> BotoS3Client:
    """Build a boto3 S3 client with appropriate timeouts for the use case.

    Args:
        s3_config: S3 configuration with credentials/region.
        endpoint_url: Resolved endpoint URL (host or docker context).
        purpose: Client purpose affecting timeout configuration:
            - "default": Standard timeouts for data operations
            - "healthcheck": Aggressive timeouts for fast-fail probes

    Returns:
        Configured boto3 S3 client.

    Example:
        >>> from skill_radar.platform.runtime import get_runtime_context, resolve_s3_endpoint
        >>> ctx = get_runtime_context()
        >>> endpoint = resolve_s3_endpoint(s3_config, ctx)
        >>> client = build_s3_client(s3_config, endpoint, purpose="healthcheck")
    """
    config = _HEALTHCHECK_TIMEOUTS if purpose == "healthcheck" else _DEFAULT_TIMEOUTS

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=s3_config.region,
        use_ssl=s3_config.secure,
        config=config,
    )


def check_s3_connectivity(
    s3_config: S3Config,
    endpoint_url: str,
) -> tuple[bool, str]:
    """Fast connectivity check to S3/MinIO endpoint.

    Uses healthcheck-optimized client with tight timeouts.
    Provides specific error messages for common failure modes:
    - Missing credentials (NoCredentialsError)
    - Unreachable endpoint (EndpointConnectionError)
    - S3/MinIO errors (ClientError with error code)

    Args:
        s3_config: S3 configuration.
        endpoint_url: Resolved endpoint URL to check.

    Returns:
        Tuple of (success: bool, detail: str)
        - On success: (True, "Connected to {endpoint}")
        - On failure: (False, "{specific error message}")
    """
    try:
        client = build_s3_client(s3_config, endpoint_url, purpose="healthcheck")
        # list_buckets is a lightweight operation that verifies connectivity
        client.list_buckets()
        return True, f"Connected to {endpoint_url}"
    except NoCredentialsError:
        return False, (
            "Missing AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or source .env"
        )
    except EndpointConnectionError as exc:
        return False, f"Cannot reach endpoint {endpoint_url}: {exc}"
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        error_msg = exc.response.get("Error", {}).get("Message", str(exc))
        return False, f"S3 error {error_code}: {error_msg}"
    except Exception as exc:
        # Catch other connection errors, timeouts, etc.
        return False, f"Connection failed: {type(exc).__name__}: {exc}"
