"""Tests for object store implementations."""

from collections.abc import Mapping
from typing import Any
from unittest.mock import Mock

from commoncrawl_pipeline.objectstore import MinIOObjectStore, ObjectStore


class FakeObjectStore(ObjectStore):
    """Fake object store for testing."""

    def __init__(self):
        self.documents = []
        self.bucket_exists = False

    def write_document(self, document: Mapping[str, Any]) -> None:
        self.documents.append(document)

    def ensure_bucket_exists(self) -> None:
        self.bucket_exists = True

    def flush(self) -> None:
        """Fake flush does nothing."""
        pass


def test_fake_object_store_writes_documents():
    """Test that fake object store accumulates documents."""
    store = FakeObjectStore()
    store.ensure_bucket_exists()

    doc1 = {"url": "http://example.com", "text": "Hello"}
    doc2 = {"url": "http://test.com", "text": "World"}

    store.write_document(doc1)
    store.write_document(doc2)

    assert len(store.documents) == 2
    assert store.documents[0] == doc1
    assert store.documents[1] == doc2
    assert store.bucket_exists is True


def test_minio_object_store_initialization():
    """Test MinIO object store initialization."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )

    assert store.bucket_name == "test-bucket"
    assert store.buffer == []
    assert store.buffer_size_bytes == 0
    assert store.max_buffer_size_bytes == 5 * 1024 * 1024


def test_minio_object_store_write_document_buffers():
    """Test that documents are buffered before flushing."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()

    doc = {"url": "http://example.com", "text": "Hello"}
    store.write_document(doc)

    # Document should be buffered, not yet written to MinIO
    assert len(store.buffer) == 1
    store.client.put_object.assert_not_called()


def test_minio_object_store_flush_writes_to_minio():
    """Test that flush writes buffered documents to MinIO."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()

    doc1 = {"url": "http://example.com", "text": "Hello"}
    doc2 = {"url": "http://test.com", "text": "World"}

    store.write_document(doc1)
    store.write_document(doc2)
    store.flush()

    # After flush, buffer should be empty
    assert len(store.buffer) == 0
    assert store.buffer_size_bytes == 0

    # put_object should have been called once
    store.client.put_object.assert_called_once()
    call_args = store.client.put_object.call_args

    assert call_args.kwargs["bucket_name"] == "test-bucket"
    assert call_args.kwargs["content_type"] == "application/x-ndjson"
    # Object name should be in documents/ directory and end with .jsonl
    assert call_args.kwargs["object_name"].startswith("documents/")
    assert call_args.kwargs["object_name"].endswith(".jsonl")


def test_minio_object_store_auto_flush_on_buffer_size():
    """Test that buffer automatically flushes when it exceeds max size."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()
    store.max_buffer_size_bytes = 100  # Set low threshold for testing

    # Write a document larger than threshold
    large_doc = {"url": "http://example.com", "text": "x" * 200}
    store.write_document(large_doc)

    # Should have triggered automatic flush
    store.client.put_object.assert_called_once()
    # Buffer should be empty after auto-flush
    assert len(store.buffer) == 0


def test_minio_object_store_ensure_bucket_exists():
    """Test bucket creation logic."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()
    store.client.bucket_exists.return_value = False

    store.ensure_bucket_exists()

    store.client.bucket_exists.assert_called_once_with("test-bucket")
    store.client.make_bucket.assert_called_once_with("test-bucket")


def test_minio_object_store_ensure_bucket_exists_already_exists():
    """Test that bucket is not created if it already exists."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()
    store.client.bucket_exists.return_value = True

    store.ensure_bucket_exists()

    store.client.bucket_exists.assert_called_once_with("test-bucket")
    store.client.make_bucket.assert_not_called()


def test_minio_object_store_flush_empty_buffer():
    """Test that flushing an empty buffer does nothing."""
    store = MinIOObjectStore(
        endpoint="localhost:9000",
        access_key="testkey",
        secret_key="testsecret",
        bucket_name="test-bucket",
        secure=False,
    )
    store.client = Mock()

    store.flush()

    store.client.put_object.assert_not_called()
