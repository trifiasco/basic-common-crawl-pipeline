import csv
import gzip
import logging
import time
from abc import ABC, abstractmethod

import requests
from requests.exceptions import RequestException, Timeout

logger = logging.getLogger(__name__)


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(
        self,
        base_url: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        """Download and decompress data with retry logic.

        Args:
            url: URL path to download
            start: Byte offset to start from
            length: Number of bytes to download

        Returns:
            Decompressed content as bytes

        Raises:
            RequestException: If download fails after all retries
            gzip.BadGzipFile: If decompression fails
        """
        headers = {"Range": f"bytes={start}-{start + length - 1}"}
        full_url = f"{self.base_url}/{url}"

        for attempt in range(self.max_retries):
            try:
                response = requests.get(full_url, headers=headers, timeout=self.timeout)
                response.raise_for_status()
                buffer = response.content
                return gzip.decompress(buffer)

            except Timeout as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        "Download timeout, retrying...",
                        extra={
                            "url": url,
                            "attempt": attempt + 1,
                            "max_retries": self.max_retries,
                            "retry_delay": delay,
                        },
                    )
                    time.sleep(delay)
                else:
                    raise RequestException(
                        f"Download timed out after {self.max_retries} attempts: {url}"
                    ) from e

            except requests.HTTPError as e:
                # Don't retry on client errors (4xx), but retry on server errors (5xx)
                if e.response is not None and 400 <= e.response.status_code < 500:
                    raise  # Client error, don't retry
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        "HTTP error, retrying...",
                        extra={
                            "url": url,
                            "status_code": e.response.status_code
                            if e.response
                            else "unknown",
                            "attempt": attempt + 1,
                            "max_retries": self.max_retries,
                            "retry_delay": delay,
                        },
                        exc_info=True,
                    )
                    time.sleep(delay)
                else:
                    raise

            except RequestException:
                # Network errors, connection errors, etc.
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        "Network error, retrying...",
                        extra={
                            "url": url,
                            "attempt": attempt + 1,
                            "max_retries": self.max_retries,
                            "retry_delay": delay,
                        },
                        exc_info=True,
                    )
                    time.sleep(delay)
                else:
                    raise

        # This should never be reached, but just in case
        raise RequestException(
            f"Download failed after {self.max_retries} attempts: {url}"
        )


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def get_progress_percentage(self) -> float:
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.file = open(filename)
        # Count total lines for progress tracking
        self.total_lines = sum(1 for _ in self.file)
        self.file.seek(0)  # Seek back to start
        self.reader = csv.reader(self.file, delimiter="\t")
        self.lines_processed = 0

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        self.lines_processed += 1
        return row

    def get_progress_percentage(self) -> float:
        """Returns the percentage of file processed (0-100)"""
        if self.total_lines == 0:
            return 100.0
        return (self.lines_processed / self.total_lines) * 100

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close file."""
        self.file.close()
        return False

    def __del__(self) -> None:
        """Cleanup in case context manager is not used."""
        if hasattr(self, "file") and not self.file.closed:
            self.file.close()
