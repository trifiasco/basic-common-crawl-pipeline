"""Configuration module for Common Crawl Pipeline.

Loads environment variables from .env file using python-dotenv.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env file from the python directory (parent of src)
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


# RabbitMQ Configuration
RABBITMQ_CONNECTION_STRING = os.getenv(
    "RABBITMQ_CONNECTION_STRING", "amqp://guest:guest@localhost:5672"
)
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_NAME", "batches")

# Prometheus Metrics Ports
BATCHER_METRICS_PORT = int(os.getenv("BATCHER_METRICS_PORT", "9000"))
WORKER_METRICS_PORT = int(os.getenv("WORKER_METRICS_PORT", "9001"))

# Common Crawl Configuration
CC_BASE_URL = os.getenv("CC_BASE_URL", "https://data.commoncrawl.org")
CC_CRAWL_PATH = os.getenv(
    "CC_CRAWL_PATH", "cc-index/collections/CC-MAIN-2024-30/indexes"
)

# Processing Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
WORKER_PREFETCH_COUNT = int(os.getenv("WORKER_PREFETCH_COUNT", "1"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Object Store Configuration (MinIO)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "commoncrawl-documents")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUFFER_SIZE_MB = int(os.getenv("MINIO_BUFFER_SIZE_MB", "5"))  # MB

# Retry Configuration
DOWNLOAD_MAX_RETRIES = int(os.getenv("DOWNLOAD_MAX_RETRIES", "3"))
DOWNLOAD_RETRY_DELAY = float(os.getenv("DOWNLOAD_RETRY_DELAY", "1.0"))  # seconds
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))  # seconds
RABBITMQ_MAX_RETRIES = int(os.getenv("RABBITMQ_MAX_RETRIES", "5"))
RABBITMQ_RETRY_DELAY = float(os.getenv("RABBITMQ_RETRY_DELAY", "2.0"))  # seconds
