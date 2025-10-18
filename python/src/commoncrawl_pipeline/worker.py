import io
import json
import logging
import signal
import sys
from datetime import UTC, datetime

import trafilatura
from prometheus_client import Counter, start_http_server
from requests.exceptions import RequestException
from warcio.archiveiterator import WARCIterator

from commoncrawl_pipeline.commoncrawl import CCDownloader, Downloader
from commoncrawl_pipeline.config import (
    CC_BASE_URL,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_RETRY_DELAY,
    DOWNLOAD_TIMEOUT,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_NAME,
    MINIO_BUFFER_SIZE_MB,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    QUEUE_NAME,
    WORKER_METRICS_PORT,
    WORKER_PREFETCH_COUNT,
)
from commoncrawl_pipeline.objectstore import MinIOObjectStore, ObjectStore
from commoncrawl_pipeline.rabbitmq import rabbitmq_channel

logger = logging.getLogger(__name__)

batch_counter = Counter("worker_batches", "Number of consumed batches")
batch_items_total = Counter(
    "worker_batch_items_total", "Total items received in batches"
)
batch_items_processed = Counter(
    "worker_batch_items_processed", "Batch items successfully processed"
)
download_bytes_total = Counter(
    "worker_download_bytes_total", "Total bytes downloaded from Common Crawl"
)
warc_records_total = Counter(
    "worker_warc_records_total", "Total WARC records processed"
)
warc_records_response = Counter(
    "worker_warc_records_response", "WARC records that are responses"
)
warc_records_non_response = Counter(
    "worker_warc_records_non_response", "WARC records filtered (not response type)"
)
documents_extracted = Counter(
    "worker_documents_extracted", "Documents successfully extracted with trafilatura"
)
documents_extraction_failed = Counter(
    "worker_documents_extraction_failed",
    "Documents where trafilatura extraction returned None",
)
documents_written = Counter(
    "worker_documents_written", "Documents successfully written to object store"
)
upload_bytes_total = Counter(
    "worker_upload_bytes_total", "Total bytes uploaded to object store"
)
upload_errors = Counter(
    "worker_upload_errors", "Errors encountered while uploading to object store"
)
batch_item_download_errors = Counter(
    "worker_batch_item_download_errors", "Batch items that failed to download"
)
batch_item_parse_errors = Counter(
    "worker_batch_item_parse_errors", "Batch items with parse/decode errors"
)
batch_processing_errors = Counter(
    "worker_batch_processing_errors", "Batches that failed processing entirely"
)


def process_batch(
    downloader: Downloader, object_store: ObjectStore, ch, method, _properties, body
):
    """Process a batch of items with error isolation.

    Each item in the batch is processed independently. If one item fails,
    we continue processing the rest. The batch is only ACKed if we successfully
    process at least some items.
    """
    try:
        logger.info("Received batch", extra={"batch_size": len(body)})
        batch = json.loads(body)
        batch_items_total.inc(len(batch))
    except (json.JSONDecodeError, TypeError):
        logger.error("Failed to parse batch message", exc_info=True)
        batch_processing_errors.inc()
        # NACK and requeue - message might be corrupted temporarily
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    items_processed = 0
    items_failed = 0

    for item in batch:
        try:
            # Download WARC chunk with retry logic
            try:
                data = downloader.download_and_unzip(
                    item["metadata"]["filename"],
                    int(item["metadata"]["offset"]),
                    int(item["metadata"]["length"]),
                )
                download_bytes_total.inc(len(data))
            except RequestException:
                logger.error(
                    "Download failed for WARC chunk",
                    extra={"filename": item["metadata"]["filename"]},
                    exc_info=True,
                )
                batch_item_download_errors.inc()
                items_failed += 1
                continue  # Skip this item, continue with next

            # Process WARC records
            try:
                for record in WARCIterator(io.BytesIO(data)):
                    warc_records_total.inc()
                    if record.rec_type == "response":
                        warc_records_response.inc()
                        try:
                            text = trafilatura.extract(record.content_stream().read())
                            if text is not None:
                                documents_extracted.inc()
                                # Create document with metadata and extracted text
                                document = {
                                    "url": item["metadata"].get("url", ""),
                                    "surt_url": item.get("surt_url", ""),
                                    "timestamp": item.get("timestamp", ""),
                                    "extracted_text": text,
                                    "digest": item["metadata"].get("digest", ""),
                                    "mime": item["metadata"].get("mime", ""),
                                    "status": item["metadata"].get("status", ""),
                                    "languages": item["metadata"].get("languages", ""),
                                    "extraction_timestamp": datetime.now(
                                        UTC
                                    ).isoformat(),
                                }
                                try:
                                    object_store.write_document(document)
                                    documents_written.inc()
                                    # Track approximate JSON size
                                    upload_bytes_total.inc(len(json.dumps(document)))
                                except Exception:
                                    upload_errors.inc()
                                    logger.error(
                                        "Failed to write document to object store",
                                        exc_info=True,
                                    )
                                    # Continue processing other documents
                            else:
                                documents_extraction_failed.inc()
                        except Exception:
                            logger.error("Text extraction failed", exc_info=True)
                            documents_extraction_failed.inc()
                            # Continue with next record
                    else:
                        warc_records_non_response.inc()
            except Exception:
                logger.error("WARC processing failed", exc_info=True)
                batch_item_parse_errors.inc()
                items_failed += 1
                continue

            batch_items_processed.inc()
            items_processed += 1

        except Exception:
            logger.error("Unexpected error processing batch item", exc_info=True)
            items_failed += 1
            batch_item_parse_errors.inc()
            # Continue with next item

    # Flush object store buffer after processing all items
    # If flush fails, NACK the batch so RabbitMQ can retry or send to DLQ
    try:
        object_store.flush()
    except Exception:
        logger.error("Failed to flush object store", exc_info=True)
        upload_errors.inc()
        batch_processing_errors.inc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # ACK the batch if we processed at least some items successfully
    if items_processed > 0:
        batch_counter.inc()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(
            "Batch processed",
            extra={"items_succeeded": items_processed, "items_failed": items_failed},
        )
    else:
        # All items failed - NACK without requeue (send to DLQ if configured)
        logger.warning("Batch failed completely", extra={"items_failed": items_failed})
        batch_processing_errors.inc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    start_http_server(WORKER_METRICS_PORT)
    downloader = CCDownloader(
        CC_BASE_URL,
        max_retries=DOWNLOAD_MAX_RETRIES,
        retry_delay=DOWNLOAD_RETRY_DELAY,
        timeout=DOWNLOAD_TIMEOUT,
    )

    # Initialize object store
    object_store = MinIOObjectStore(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=MINIO_BUCKET_NAME,
        secure=MINIO_SECURE,
        buffer_size_mb=MINIO_BUFFER_SIZE_MB,
    )
    object_store.ensure_bucket_exists()

    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=WORKER_PREFETCH_COUNT)

    # Setup graceful shutdown handlers
    def signal_handler(signum, frame):
        logger.info(
            "Received signal, shutting down gracefully...", extra={"signal": signum}
        )
        try:
            # Flush any buffered documents
            logger.info("Flushing object store buffer...")
            object_store.flush()
            # Stop consuming new messages
            logger.info("Stopping message consumption...")
            channel.stop_consuming()
            # Close RabbitMQ connection
            logger.info("Closing RabbitMQ connection...")
            if channel.connection and channel.connection.is_open:
                channel.connection.close()
            logger.info("Graceful shutdown complete")
        except Exception:
            logger.error("Error during shutdown", exc_info=True)
        finally:
            sys.exit(0)

    # Register signal handlers for SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Worker started. Press Ctrl+C to stop gracefully.")
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, object_store, ch, method, properties, body
        ),
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        # This shouldn't be reached due to signal handler, but just in case
        signal_handler(signal.SIGINT, None)


if __name__ == "__main__":
    main()
