"""Prometheus metrics for monitoring the Common Crawl pipeline.

All metrics are defined here for centralized management and easy discovery.
"""

from prometheus_client import Counter, Gauge

# Batcher metrics - URL filtering and batch publishing
batcher_batches = Counter("batcher_batches", "Number of published batches")
batcher_documents_total = Counter(
    "batcher_documents_total", "Total documents processed"
)
batcher_documents_empty_filtered = Counter(
    "batcher_documents_empty_filtered", "Documents filtered due to empty lines"
)
batcher_documents_language_filtered = Counter(
    "batcher_documents_language_filtered",
    "Documents filtered due to non-English language",
)
batcher_documents_status_filtered = Counter(
    "batcher_documents_status_filtered", "Documents filtered due to non-200 status"
)
batcher_documents_accepted = Counter(
    "batcher_documents_accepted", "Documents accepted and published"
)

# Batcher progress tracking
batcher_cluster_idx_progress = Gauge(
    "batcher_cluster_idx_progress_percent",
    "Percentage of cluster.idx file processed (0-100)",
)
batcher_cdx_chunks_processed = Counter(
    "batcher_cdx_chunks_processed", "Number of CDX chunks processed from cluster.idx"
)
batcher_cdx_chunks_total = Gauge(
    "batcher_cdx_chunks_total", "Total number of CDX chunks in cluster.idx"
)

# Batcher error tracking
batcher_cdx_download_errors = Counter(
    "batcher_cdx_download_errors", "CDX chunk download failures"
)
batcher_cdx_parse_errors = Counter(
    "batcher_cdx_parse_errors", "CDX chunk parse/decode errors"
)
batcher_json_parse_errors = Counter(
    "batcher_json_parse_errors", "JSON metadata parse errors"
)
batcher_publish_errors = Counter("batcher_publish_errors", "RabbitMQ publish failures")

# Worker metrics - Batch processing
worker_batches = Counter("worker_batches", "Number of consumed batches")
worker_batch_items_total = Counter(
    "worker_batch_items_total", "Total items received in batches"
)
worker_batch_items_processed = Counter(
    "worker_batch_items_processed", "Batch items successfully processed"
)

# Worker metrics - Download and data volume
worker_download_bytes_total = Counter(
    "worker_download_bytes_total", "Total bytes downloaded from Common Crawl"
)
worker_warc_records_total = Counter(
    "worker_warc_records_total", "Total WARC records processed"
)
worker_warc_records_response = Counter(
    "worker_warc_records_response", "WARC records that are responses"
)
worker_warc_records_non_response = Counter(
    "worker_warc_records_non_response", "WARC records filtered (not response type)"
)

# Worker metrics - Text extraction
worker_documents_extracted = Counter(
    "worker_documents_extracted", "Documents successfully extracted with trafilatura"
)
worker_documents_extraction_failed = Counter(
    "worker_documents_extraction_failed",
    "Documents where trafilatura extraction returned None",
)

# Worker metrics - Object storage
worker_documents_written = Counter(
    "worker_documents_written", "Documents successfully written to object store"
)
worker_upload_bytes_total = Counter(
    "worker_upload_bytes_total", "Total bytes uploaded to object store"
)
worker_upload_errors = Counter(
    "worker_upload_errors", "Errors encountered while uploading to object store"
)

# Worker error tracking
worker_batch_item_download_errors = Counter(
    "worker_batch_item_download_errors", "Batch items that failed to download"
)
worker_batch_item_parse_errors = Counter(
    "worker_batch_item_parse_errors", "Batch items with parse/decode errors"
)
worker_batch_processing_errors = Counter(
    "worker_batch_processing_errors", "Batches that failed processing entirely"
)
