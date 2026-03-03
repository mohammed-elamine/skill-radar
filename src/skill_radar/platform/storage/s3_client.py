"""Thin S3 abstraction over boto3, compatible with MinIO.

Credentials are resolved from the standard boto3 chain
(``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` environment variables).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import boto3
from botocore.exceptions import ClientError

from .exceptions import ObjectAlreadyExistsError, ObjectNotFoundError, UploadError

if TYPE_CHECKING:
    from pathlib import Path

    from skill_radar.config.models import PlatformSettings, S3Config

logger = logging.getLogger(__name__)


class S3Client:
    """Gateway for all S3 / MinIO object-storage operations.

    Parameters
    ----------
    config:
        Platform configuration providing bucket name and endpoint.
    """

    def __init__(self, config: PlatformSettings) -> None:
        s3_cfg: S3Config = config.storage.s3
        self._bucket: str = s3_cfg.bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=s3_cfg.endpoint,
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
