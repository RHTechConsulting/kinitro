"""
Utilities for uploading evaluator job logs to S3-compatible storage.

The orchestrator serializes evaluation summaries and detailed metrics into JSON
documents that miners can inspect. This module handles converting those payloads
into stored objects so the backend can surface presigned download links without
exposing validator storage credentials.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from core.log import get_logger
from core.storage import S3Config

logger = get_logger(__name__)


class EvaluationLogUploader:
    """Upload serialized evaluation log bundles to object storage."""

    def __init__(self, config: S3Config, prefix: str = "evaluation-logs") -> None:
        self.config = config
        self.bucket_name = config.bucket_name
        self.prefix = prefix.rstrip("/")

        self._s3_client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name=config.region,
        )

    def build_object_key(
        self, submission_id: int | str, job_id: int | str, suffix: Optional[str] = None
    ) -> str:
        """Create a deterministic object key for a job's log bundle."""
        sub_id_str = str(submission_id)
        job_id_str = str(job_id)
        suffix_part = f"-{suffix}" if suffix else ""
        return f"{self.prefix}/{sub_id_str}/{job_id_str}{suffix_part}.json"

    def upload_log_bundle(
        self,
        *,
        submission_id: int | str,
        job_id: int | str,
        payload: Dict[str, Any],
        suffix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Upload a JSON payload describing an evaluation run.

        Returns metadata describing where the bundle was stored so callers can
        surface links to miners.
        """
        if not payload:
            raise ValueError("Cannot upload empty evaluation log payload")

        object_key = self.build_object_key(submission_id, job_id, suffix)
        body = json.dumps(
            payload, ensure_ascii=True, separators=(",", ":"), sort_keys=False
        ).encode("utf-8")

        try:
            self._s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=body,
                ContentType="application/json",
            )
        except (BotoCoreError, ClientError, Exception):
            logger.exception(
                "Failed to upload evaluation log bundle for job %s", job_id
            )
            raise

        uploaded_at = datetime.now(timezone.utc).isoformat()
        metadata: Dict[str, Any] = {
            "bucket": self.bucket_name,
            "object_key": object_key,
            "uploaded_at": uploaded_at,
        }

        public_url = self._public_url(object_key)
        if public_url:
            metadata["public_url"] = public_url

        return metadata

    def _public_url(self, object_key: str) -> Optional[str]:
        if not self.config.public_url_base:
            return None
        return f"{self.config.public_url_base.rstrip('/')}/{object_key}"
