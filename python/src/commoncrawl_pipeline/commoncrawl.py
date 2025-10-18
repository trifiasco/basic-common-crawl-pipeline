import csv
import gzip
from abc import ABC, abstractmethod

import requests


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start + length - 1}"}
        response = requests.get(f"{self.base_url}/{url}", headers=headers)
        response.raise_for_status()
        buffer = response.content
        return gzip.decompress(buffer)


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

    def __del__(self) -> None:
        self.file.close()


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
        ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
        [
            "101,141,199,66)/robots.txt 20240714155331",
            "cdx-00000.gz",
            "188224",
            "178351",
            "2",
        ],
        ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
    ]
