import argparse
import json
import logging
import time
from collections.abc import Mapping, Sequence
from typing import Any

from pika import BasicProperties
from pika.exceptions import AMQPConnectionError, AMQPError
from prometheus_client import Counter, Gauge, start_http_server
from requests.exceptions import RequestException

from commoncrawl_pipeline.commoncrawl import (
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
)
from commoncrawl_pipeline.config import (
    BATCH_SIZE,
    BATCHER_METRICS_PORT,
    CC_BASE_URL,
    CC_CRAWL_PATH,
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_RETRY_DELAY,
    DOWNLOAD_TIMEOUT,
    QUEUE_NAME,
    RABBITMQ_MAX_RETRIES,
    RABBITMQ_RETRY_DELAY,
)
from commoncrawl_pipeline.logging_config import setup_logging
from commoncrawl_pipeline.rabbitmq import MessageQueueChannel, RabbitMQChannel

logger = logging.getLogger(__name__)

batch_counter = Counter("batcher_batches", "Number of published batches")
documents_total = Counter("batcher_documents_total", "Total documents processed")
documents_empty_filtered = Counter(
    "batcher_documents_empty_filtered", "Documents filtered due to empty lines"
)
documents_language_filtered = Counter(
    "batcher_documents_language_filtered",
    "Documents filtered due to non-English language",
)
documents_status_filtered = Counter(
    "batcher_documents_status_filtered", "Documents filtered due to non-200 status"
)
documents_accepted = Counter(
    "batcher_documents_accepted", "Documents accepted and published"
)

# Progress tracking metrics
cluster_idx_progress = Gauge(
    "batcher_cluster_idx_progress_percent",
    "Percentage of cluster.idx file processed (0-100)",
)
cdx_chunks_processed = Counter(
    "batcher_cdx_chunks_processed", "Number of CDX chunks processed from cluster.idx"
)
cdx_chunks_total = Gauge(
    "batcher_cdx_chunks_total", "Total number of CDX chunks in cluster.idx"
)

# Error tracking metrics
cdx_download_errors = Counter(
    "batcher_cdx_download_errors", "CDX chunk download failures"
)
cdx_parse_errors = Counter("batcher_cdx_parse_errors", "CDX chunk parse/decode errors")
json_parse_errors = Counter("batcher_json_parse_errors", "JSON metadata parse errors")
publish_errors = Counter("batcher_publish_errors", "RabbitMQ publish failures")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> tuple[bool, bool]:
    """Publish a batch to RabbitMQ.

    Args:
        channel: Message queue channel
        batch: Sequence of documents to publish

    Returns:
        Tuple of (success, is_connection_error)
        - success: True if publish succeeded, False otherwise
        - is_connection_error: True if error was due to connection failure
    """
    try:
        logger.info("Publishing batch", extra={"batch_size": len(batch)})
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(batch),
            properties=BasicProperties(delivery_mode=2),  # Make message persistent
        )
        batch_counter.inc()
        return True, False
    except AMQPConnectionError:
        logger.error("RabbitMQ connection lost while publishing batch", exc_info=True)
        publish_errors.inc()
        return False, True  # Connection error - need to reconnect
    except AMQPError:
        logger.error("Failed to publish batch to RabbitMQ", exc_info=True)
        publish_errors.inc()
        return False, False
    except Exception:
        logger.error("Unexpected error publishing batch", exc_info=True)
        publish_errors.inc()
        return False, False


def process_index(
    index: IndexReader,
    get_channel_func,
    downloader: Downloader,
    batch_size: int,
) -> None:
    """Process CDX index.

    Downloads CDX chunks, parses them, filters documents, and publishes batches.
    Continues processing even if individual chunks or lines fail.
    Reconnects to RabbitMQ if connection is lost.

    Args:
        index: CDX index reader
        get_channel_func: Function to get/reconnect RabbitMQ channel
        downloader: Downloader for CDX chunks
        batch_size: Number of documents per batch
    """
    found_urls = []
    channel = get_channel_func()

    for cdx_chunk in index:
        cdx_chunks_processed.inc()

        # Download CDX chunk with error handling
        try:
            data = downloader.download_and_unzip(
                cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
            )
        except RequestException:
            logger.error(
                "Failed to download CDX chunk",
                extra={"cdx_file": cdx_chunk[1]},
                exc_info=True,
            )
            cdx_download_errors.inc()
            continue  # Skip this chunk, continue with next

        # Decode CDX data
        try:
            decoded_data = data.decode("utf-8")
        except UnicodeDecodeError:
            logger.error(
                "Failed to decode CDX chunk",
                extra={"cdx_file": cdx_chunk[1]},
                exc_info=True,
            )
            cdx_parse_errors.inc()
            continue

        # Process each line in the CDX chunk
        for line in decoded_data.split("\n"):
            documents_total.inc()

            # Skip empty lines
            if line == "":
                documents_empty_filtered.inc()
                continue

            # Parse CDX line
            try:
                values = line.split(" ")
                if len(values) < 3:
                    logger.warning(
                        "Malformed CDX line (too few fields)",
                        extra={"line_preview": line[:100]},
                    )
                    cdx_parse_errors.inc()
                    continue

                # Parse JSON metadata
                try:
                    metadata = json.loads("".join(values[2:]))
                except json.JSONDecodeError:
                    logger.warning(
                        "Failed to parse JSON metadata",
                        extra={"line_preview": line[:100]},
                        exc_info=True,
                    )
                    json_parse_errors.inc()
                    continue

                # Validate metadata structure
                if not isinstance(metadata, dict):
                    logger.warning(
                        "Metadata is not a dict",
                        extra={"metadata_type": type(metadata).__name__},
                    )
                    json_parse_errors.inc()
                    continue

            except Exception:
                logger.error("Unexpected error parsing CDX line", exc_info=True)
                cdx_parse_errors.inc()
                continue

            # Check language filter
            if "languages" not in metadata or "eng" not in metadata["languages"]:
                documents_language_filtered.inc()
                continue

            # Check status filter
            if metadata.get("status") != "200":
                documents_status_filtered.inc()
                continue

            # Document accepted
            documents_accepted.inc()
            found_urls.append(
                {
                    "surt_url": values[0],
                    "timestamp": values[1],
                    "metadata": metadata,
                }
            )

            # Publish batch when full
            if len(found_urls) >= batch_size:
                success, is_conn_error = publish_batch(channel, found_urls)
                if success:
                    found_urls = []
                elif is_conn_error:
                    # Reconnect to RabbitMQ and retry
                    logger.warning("RabbitMQ connection lost, reconnecting...")
                    channel = get_channel_func()
                    success, _ = publish_batch(channel, found_urls)
                    if success:
                        found_urls = []
                    else:
                        logger.warning(
                            "Batch publish failed after reconnect, will retry with next batch"
                        )
                else:
                    # Non-connection error, keep accumulating
                    logger.warning("Batch publish failed, will retry with next batch")

        # Update progress after processing each CDX chunk
        cluster_idx_progress.set(index.get_progress_percentage())

    # Publish remaining documents
    if len(found_urls) > 0:
        success, is_conn_error = publish_batch(channel, found_urls)
        if not success and is_conn_error:
            # Reconnect and retry final batch
            logger.warning("RabbitMQ connection lost, reconnecting for final batch...")
            channel = get_channel_func()
            publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    setup_logging()
    start_http_server(BATCHER_METRICS_PORT)

    # Connection factory function for reconnection support
    def get_rabbitmq_channel() -> MessageQueueChannel:
        """Get RabbitMQ channel with retry logic."""
        for attempt in range(RABBITMQ_MAX_RETRIES):
            try:
                return RabbitMQChannel()
            except AMQPConnectionError as e:
                if attempt < RABBITMQ_MAX_RETRIES - 1:
                    delay = RABBITMQ_RETRY_DELAY * (2**attempt)
                    logger.warning(
                        "Failed to connect to RabbitMQ, retrying...",
                        extra={
                            "attempt": attempt + 1,
                            "max_retries": RABBITMQ_MAX_RETRIES,
                            "retry_delay": delay,
                        },
                        exc_info=True,
                    )
                    time.sleep(delay)
                else:
                    raise AMQPConnectionError(
                        f"Failed to connect to RabbitMQ after {RABBITMQ_MAX_RETRIES} attempts"
                    ) from e
        raise AMQPConnectionError(
            f"Failed to connect to RabbitMQ after {RABBITMQ_MAX_RETRIES} attempts"
        )

    # Batcher downloads CDX index files from the indexes directory
    downloader = CCDownloader(
        f"{CC_BASE_URL}/{CC_CRAWL_PATH}",
        max_retries=DOWNLOAD_MAX_RETRIES,
        retry_delay=DOWNLOAD_RETRY_DELAY,
        timeout=DOWNLOAD_TIMEOUT,
    )

    with CSVIndexReader(args.cluster_idx_filename) as index_reader:
        # Set total CDX chunks for progress tracking
        cdx_chunks_total.set(index_reader.total_lines)
        process_index(index_reader, get_rabbitmq_channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
