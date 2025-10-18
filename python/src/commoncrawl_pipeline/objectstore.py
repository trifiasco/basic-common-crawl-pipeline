"""Object store abstraction for storing extracted documents."""

import contextlib
import io
import json
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from minio import Minio
from minio.error import S3Error


class ObjectStore(ABC):
    """Abstract base class for object store implementations."""

    @abstractmethod
    def write_document(self, document: Mapping[str, Any]) -> None:
        """Write a document to the object store.

        Args:
            document: Document data to write
        """
        pass

    @abstractmethod
    def ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create if necessary."""
        pass

    @abstractmethod
    def flush(self) -> None:
        """Flush any buffered documents to the object store."""
        pass


class MinIOObjectStore(ObjectStore):
    """MinIO object store implementation that writes JSONL files."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False,
        buffer_size_mb: int = 5,
    ) -> None:
        """Initialize MinIO object store.

        Args:
            endpoint: MinIO endpoint (e.g., 'localhost:9000')
            access_key: Access key
            secret_key: Secret key
            bucket_name: Bucket name
            secure: Use HTTPS (default: False for local MinIO)
            buffer_size_mb: Buffer size in megabytes before flushing (default: 5)
        """
        self.bucket_name = bucket_name
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        # Buffer for batching writes
        self.buffer: list[str] = []
        self.buffer_size_bytes = 0
        self.max_buffer_size_bytes = buffer_size_mb * 1024 * 1024  # Convert MB to bytes
        self.current_file_key = self._generate_file_key()

    def _generate_file_key(self) -> str:
        """Generate a unique file key for the current batch."""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        return f"documents/{timestamp}.jsonl"

    def ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create if necessary."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as e:
            raise RuntimeError(f"Failed to ensure bucket exists: {e}") from e

    def write_document(self, document: Mapping[str, Any]) -> None:
        """Write a document to the buffer, flush if buffer is full.

        Args:
            document: Document data to write
        """
        json_line = json.dumps(document) + "\n"
        json_bytes = json_line.encode("utf-8")

        self.buffer.append(json_line)
        self.buffer_size_bytes += len(json_bytes)

        # Flush if buffer exceeds threshold
        if self.buffer_size_bytes >= self.max_buffer_size_bytes:
            self.flush()

    def flush(self) -> None:
        """Flush buffered documents to MinIO."""
        if not self.buffer:
            return

        content = "".join(self.buffer).encode("utf-8")
        content_stream = io.BytesIO(content)

        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=self.current_file_key,
            data=content_stream,
            length=len(content),
            content_type="application/x-ndjson",
        )

        # Reset buffer and generate new file key
        self.buffer = []
        self.buffer_size_bytes = 0
        self.current_file_key = self._generate_file_key()

    def __del__(self) -> None:
        """Flush remaining documents on cleanup."""
        with contextlib.suppress(Exception):
            self.flush()
