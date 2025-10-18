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


def process_batch(downloader: Downloader, ch, method, _properties, body):
    print("Received batch of size", len(body))
    batch = json.loads(body)
    batch_items_total.inc(len(batch))

    for item in batch:
        data = downloader.download_and_unzip(
            item["metadata"]["filename"],
            int(item["metadata"]["offset"]),
            int(item["metadata"]["length"]),
        )
        download_bytes_total.inc(len(data))

        for record in WARCIterator(io.BytesIO(data)):
            warc_records_total.inc()
            if record.rec_type == "response":
                warc_records_response.inc()
                text = trafilatura.extract(record.content_stream().read())
                if text is not None:
                    documents_extracted.inc()
                    # TODO: process text
                else:
                    documents_extraction_failed.inc()
            else:
                warc_records_non_response.inc()

        batch_items_processed.inc()

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
