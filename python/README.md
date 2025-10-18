# Common Crawl Pipeline - Python Implementation

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](http://mypy-lang.org/)

A modern Python implementation of a Common Crawl data processing pipeline with batcher and worker components for downloading and extracting web content at scale.

## ✨ Features

- 🚀 **Scalable Architecture**: Single batcher, multiple workers with RabbitMQ queue
- 🛠️ **Modern Tooling**: uv, ruff, mypy, pre-commit hooks, Docker Compose
- 🎯 **Production Ready**: Proper package structure, tests, type hints
- 📦 **Easy Setup**: One command to get started

## 📋 Prerequisites

- **Python 3.11+**
- **[uv](https://github.com/astral-sh/uv)** - `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Docker & Docker Compose**
- **Make**

## 🚀 Quick Start

```bash
# 1. Clone and navigate to python directory
cd basic-common-crawl/python
cp .env.example .env

# 2. Download Common Crawl index (from project root)
wget https://data.commoncrawl.org/cc-index/collections/CC-MAIN-2024-30/indexes/cluster.idx

# 3. Run full setup (installs everything, starts services)
make setup

# 4. Start the batcher
make run-batcher CLUSTER_FILE=../cluster.idx

# 5. In another terminal, start worker(s)
make run-worker
```

## 📚 Documentation

| Document | Description |
|----------|-------------|
| **[Setup Guide](docs/setup.md)** | Detailed installation and configuration instructions |
| **[Architecture](docs/architecture.md)** | System design, data flow, and codebase structure |
| **[Development](docs/development.md)** | Development workflows, testing, and best practices |

## 🏗️ Architecture Overview

```
┌──────────────┐
│ cluster.idx  │ (Downloaded locally)
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Batcher    │────▶│  RabbitMQ   │────▶│  Worker(s)   │
│  (Producer)  │     │    Queue    │     │  (Consumers) │
└──────────────┘     └─────────────┘     └──────────────┘
       │                                         │
       │ Downloads chunks                       │ Downloads chunks
       ▼                                         ▼
┌──────────────┐                         ┌──────────────┐
│  CDX Files   │                         │  WARC Files  │
│  (Metadata)  │                         │   (Content)  │
└──────────────┘                         └──────┬───────┘
                                                 │ Extracts text
                                                 ▼
                                          ┌──────────────┐
                                          │    MinIO     │
                                          │ (JSONL files)│
                                          └──────────────┘
```

**See [Architecture Documentation](docs/architecture.md) for details.**

## 🛠️ Tech Stack

| Category | Tool | Why |
|----------|------|-----|
| **Package Manager** | uv | 10-100x faster than pip |
| **Linter/Formatter** | ruff | 10-100x faster than black/flake8 |
| **Type Checker** | mypy | Industry standard |
| **Testing** | pytest | Fast and flexible |
| **Message Queue** | RabbitMQ | Reliable message delivery |
| **Monitoring** | Prometheus | Time-series metrics |
| **Object Storage** | MinIO | S3-compatible document storage |
| **Orchestration** | Docker Compose | Simple multi-service setup |


## 🎯 Common Tasks

```bash
# Development
make help           # Show all available commands
make install-dev    # Install dependencies
make test           # Run tests
make lint           # Check code quality
make format         # Auto-format code
make type-check     # Run type checker

# Running
make run-batcher CLUSTER_FILE=path/to/cluster.idx  # Start batcher
make run-worker                                     # Start worker

# Docker
make docker-up      # Start RabbitMQ + Prometheus
make docker-down    # Stop services
make docker-logs    # View logs

# Cleanup
make clean          # Remove cache files
```

## 📊 Monitoring & Access

Once running, access:

- **RabbitMQ UI**: http://localhost:15672 (guest/guest)
- **Prometheus**: http://localhost:9090
- **MinIO Console**: http://localhost:9003 (minioadmin/minioadmin)
- **MinIO API**: http://localhost:9002
- **Batcher Metrics**: http://localhost:9000/metrics
- **Worker Metrics**: http://localhost:9001/metrics

## 📁 Project Structure

```
python/
├── src/commoncrawl_pipeline/   # Main package
│   ├── __init__.py
│   ├── batcher.py              # Batcher implementation
│   ├── worker.py               # Worker implementation
│   ├── commoncrawl.py          # Common Crawl utilities
│   ├── rabbitmq.py             # RabbitMQ client
│   ├── objectstore.py          # MinIO/S3 object storage
│   ├── logging_config.py       # Logging configuration
│   └── config.py               # Configuration loader
├── tests/                      # Test suite
│   ├── test_batcher.py
│   ├── test_commoncrawl.py
│   └── test_objectstore.py
├── docs/                       # Documentation
│   ├── setup.md
│   ├── architecture.md
│   └── development.md
├── pyproject.toml              # Project config & dependencies
├── Makefile                    # Development commands
├── docker-compose.yml          # Service orchestration (RabbitMQ, Prometheus, MinIO)
├── .env                        # Environment configuration
└── .pre-commit-config.yaml     # Git hooks
```

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test
uv run pytest tests/test_batcher.py::test_filter_non_english_documents
```

## 🔧 Configuration

Copy `.env.example` to `.env` to customize:

```bash
cp .env.example .env
```

Key settings:
- `RABBITMQ_CONNECTION_STRING`: RabbitMQ URL
- `BATCH_SIZE`: URLs per batch (default: 50)
- `BATCHER_METRICS_PORT`: Batcher metrics port (default: 9000)
- `WORKER_METRICS_PORT`: Worker metrics port (default: 9001)
- `MINIO_ENDPOINT`: MinIO endpoint (default: localhost:9002)
- `MINIO_BUCKET_NAME`: Bucket for storing documents (default: commoncrawl-documents)

### 🗄️ Object Storage (MinIO)

Extracted documents are automatically stored in MinIO using **JSONL format** (JSON Lines). MinIO is included in the docker-compose setup and starts automatically with `make docker-up` or `make setup`.

**Document Format:**
Each line in the JSONL files contains a complete JSON document:
```json
{
  "url": "http://example.com/page",
  "surt_url": "com,example)/page",
  "timestamp": "20240722120756",
  "extracted_text": "The actual content...",
  "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R",
  "mime": "text/html",
  "status": "200",
  "languages": "eng",
  "extraction_timestamp": "2025-10-18T12:34:56Z"
}
```

**Storage Details:**
- Documents are buffered in memory (5MB buffer) before writing
- Files are stored as `documents/YYYYMMDD_HHMMSS_microseconds.jsonl`
- The bucket (`commoncrawl-documents`) is automatically created on first run
- Access the MinIO Console at http://localhost:9003 to browse stored files

### How It Works

- **Batcher**: Reads cluster.idx, downloads CDX file chunks, filters English + HTTP 200 URLs, publishes batches to RabbitMQ
- **Worker**: Consumes batches, downloads WARC file chunks, extracts text with trafilatura, stores documents to MinIO

**See [Architecture Documentation](docs/architecture.md) for detailed flow.**

## 🎓 Resources

- [Common Crawl Documentation](https://commoncrawl.org/get-started)
- [Project Overview Video](https://www.youtube.com/watch?v=Moy6kWmx-Os)
- [uv Documentation](https://github.com/astral-sh/uv)
- [Ruff Documentation](https://docs.astral.sh/ruff/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)

## 🔮 Future Enhancements

- [ ] Text quality filtering
- [x] Output to object storage (S3/MinIO) - **COMPLETED**
- [ ] Tokenization with Huggingface transformers
- [ ] Document length filtering
- [ ] Multi-crawl support with deduplication
- [ ] Async processing with asyncio
- [ ] Retry logic and error handling
- [ ] Distributed batcher for parallel processing

## 📝 License
MIT
