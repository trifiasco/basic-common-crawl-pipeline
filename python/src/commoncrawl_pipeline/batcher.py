import argparse
import json
from collections.abc import Mapping, Sequence
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

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
    QUEUE_NAME,
)
from commoncrawl_pipeline.rabbitmq import MessageQueueChannel, RabbitMQChannel

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    return parser.parse_args()


def publish_batch(
    channel: MessageQueueChannel,
    batch: Sequence[Mapping[str, Any]],
) -> None:
    print("Pushing batch of size", len(batch))
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(batch),
    )
    batch_counter.inc()


def process_index(
    index: IndexReader,
    channel: MessageQueueChannel,
    downloader: Downloader,
    batch_size: int,
) -> None:
    found_urls = []
    for cdx_chunk in index:
        cdx_chunks_processed.inc()
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            documents_total.inc()
            if line == "":
                documents_empty_filtered.inc()
                continue
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))

            # Check language filter
            if "languages" not in metadata or "eng" not in metadata["languages"]:
                documents_language_filtered.inc()
                continue

            # Check status filter
            if metadata["status"] != "200":
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
            if len(found_urls) >= batch_size:
                publish_batch(channel, found_urls)
                found_urls = []

        # Update progress after processing each CDX chunk
        cluster_idx_progress.set(index.get_progress_percentage())

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(BATCHER_METRICS_PORT)
    channel = RabbitMQChannel()
    # Batcher downloads CDX index files from the indexes directory
    downloader = CCDownloader(f"{CC_BASE_URL}/{CC_CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    # Set total CDX chunks for progress tracking
    cdx_chunks_total.set(index_reader.total_lines)
    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
