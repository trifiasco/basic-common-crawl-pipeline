import argparse
import json
from collections.abc import Mapping, Sequence
from typing import Any

from prometheus_client import Counter, start_http_server

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
        data = downloader.download_and_unzip(
            cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
        ).decode("utf-8")
        for line in data.split("\n"):
            if line == "":
                continue
            values = line.split(" ")
            metadata = json.loads("".join(values[2:]))
            if (
                "languages" in metadata
                and "eng" in metadata["languages"]
                and metadata["status"] == "200"
            ):
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

    if len(found_urls) > 0:
        publish_batch(channel, found_urls)


def main() -> None:
    args = parse_args()
    start_http_server(BATCHER_METRICS_PORT)
    channel = RabbitMQChannel()
    # Batcher downloads CDX index files from the indexes directory
    downloader = CCDownloader(f"{CC_BASE_URL}/{CC_CRAWL_PATH}")
    index_reader = CSVIndexReader(args.cluster_idx_filename)
    process_index(index_reader, channel, downloader, BATCH_SIZE)


if __name__ == "__main__":
    main()
