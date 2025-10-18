import io
import json

import trafilatura
from prometheus_client import Counter, start_http_server
from warcio.archiveiterator import WARCIterator

from commoncrawl_pipeline.commoncrawl import CCDownloader, Downloader
from commoncrawl_pipeline.config import (
    CC_BASE_URL,
    QUEUE_NAME,
    WORKER_METRICS_PORT,
    WORKER_PREFETCH_COUNT,
)
from commoncrawl_pipeline.rabbitmq import rabbitmq_channel

batch_counter = Counter("worker_batches", "Number of consumed batches")


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        for record in WARCIterator(io.BytesIO(data)):
            if record.rec_type == "response":
                _text = trafilatura.extract(record.content_stream().read())
                # TODO: process text
    batch_counter.inc()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    start_http_server(WORKER_METRICS_PORT)
    downloader = CCDownloader(CC_BASE_URL)
    channel = rabbitmq_channel()
    channel.basic_qos(prefetch_count=WORKER_PREFETCH_COUNT)
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=lambda ch, method, properties, body: process_batch(
            downloader, ch, method, properties, body
        ),
    )
    channel.start_consuming()


if __name__ == "__main__":
    main()
