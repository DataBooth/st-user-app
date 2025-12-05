import httpx
from pathlib import Path
import time

# import gzip
# import bz2
# import lzma


class FileDownloader:
    def __init__(self):
        self.file_size = 0
        self.download_time = 0
        self.compression_type = None

    def download_file(self, url, dir):
        start_time = time.time()

        # Create directory if it doesn't exist
        save_dir = Path(dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        # Get filename from URL
        filename = Path(url).name
        filepath = save_dir / filename

        # Download file
        with httpx.stream("GET", url) as response:
            response.raise_for_status()
            self.file_size = int(response.headers.get("Content-Length", 0))

            with filepath.open("wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)

        self.download_time = time.time() - start_time
        self._detect_compression(filepath)

        return filepath

    def _detect_compression(self, filepath):
        with filepath.open("rb") as f:
            header = f.read(8)

        if header.startswith(b"\x1f\x8b\x08"):
            self.compression_type = "gzip"
        elif header.startswith(b"BZh"):
            self.compression_type = "bzip2"
        elif header.startswith(b"\xfd7zXZ\x00"):
            self.compression_type = "lzma"
        else:
            self.compression_type = "uncompressed"

    def get_file_size(self):
        return self.file_size

    def get_download_time(self):
        return self.download_time

    def get_compression_type(self):
        return self.compression_type


if __name__ == "__main__":

    downloader = FileDownloader()
    url = "https://blobs.duckdb.org/nl-railway/services-2024.csv.gz"
    save_directory = "tmp"

    downloaded_file = downloader.download_file(url, save_directory)
    print(f"File downloaded to: {downloaded_file}")
    print(f"File size: {downloader.get_file_size()} bytes")
    print(f"Download time: {downloader.get_download_time():.2f} seconds")
    print(f"Compression type: {downloader.get_compression_type()}")
