"""Download and prepare trace data for benchmarking.

This script downloads trace data from a public S3 bucket and prepares it for use with the
load_braintrust.py and load_langsmith.py benchmark scripts.

No external dependencies required - uses Python standard library only.

Usage:
    python prepare_data.py

Examples:
    # Download trace data for benchmarking
    python prepare_data.py
"""
import gzip
import logging
import os
import sys
from urllib.request import urlopen
from urllib.error import URLError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_URL = "https://brainstore-benchmark-data.s3.us-east-2.amazonaws.com/big_traces.jsonl.gz"
DATA_DIR = "data"
TRACE_FILE = "data/big_traces.jsonl"
COMPRESSED_FILE = "data/big_traces.jsonl.gz"
CHUNK_SIZE = 8192  # 8KB chunks for efficient streaming download


def download_and_extract_traces() -> None:
    """Download and extract trace data from public S3 bucket.

    Downloads big_traces.jsonl.gz via HTTP, extracts it to data/big_traces.jsonl,
    and removes the compressed file. Uses streaming download for memory efficiency
    with large files (768MB compressed, 1.7GB uncompressed).

    Raises:
        URLError: If HTTP download fails due to network issues
        OSError: If file operations fail due to permissions or disk space
    """
    if os.path.exists(TRACE_FILE):
        logger.info(f"Trace file already exists at {TRACE_FILE}")
        file_size_mb = os.path.getsize(TRACE_FILE) / (1024 * 1024)
        logger.info(f"File size: {file_size_mb:.2f} MB")
        return

    try:
        # Create data directory if it doesn't exist
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info("Downloading trace data from public S3 bucket...")
        logger.info(f"URL: {DOWNLOAD_URL}")

        # Stream download from public S3 URL for memory efficiency
        with urlopen(DOWNLOAD_URL, timeout=30) as response:
            # Get total file size from headers
            total_size = int(response.headers.get("Content-Length", 0))
            total_size_mb = total_size / (1024 * 1024)
            logger.info(f"Downloading {total_size_mb:.2f} MB...")

            # Download in chunks to avoid loading entire file into memory
            downloaded = 0
            with open(COMPRESSED_FILE, "wb") as f:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    # Log progress every 100MB
                    if downloaded % (100 * 1024 * 1024) < CHUNK_SIZE:
                        progress_mb = downloaded / (1024 * 1024)
                        logger.info(f"Downloaded {progress_mb:.0f} MB...")

        compressed_size_mb = os.path.getsize(COMPRESSED_FILE) / (1024 * 1024)
        logger.info(f"Download complete ({compressed_size_mb:.2f} MB). Extracting...")

        # Extract the gzipped file in chunks for memory efficiency
        with gzip.open(COMPRESSED_FILE, "rb") as f_in:
            with open(TRACE_FILE, "wb") as f_out:
                while True:
                    chunk = f_in.read(CHUNK_SIZE * 1024)  # 8MB chunks for extraction
                    if not chunk:
                        break
                    f_out.write(chunk)

        extracted_size_mb = os.path.getsize(TRACE_FILE) / (1024 * 1024)
        logger.info(f"Extraction complete ({extracted_size_mb:.2f} MB)")

        # Clean up the compressed file
        os.remove(COMPRESSED_FILE)
        logger.info(f"Cleaned up compressed file")

        logger.info(f"✓ Trace data ready at {TRACE_FILE}")

    except URLError as e:
        logger.error(f"Failed to download trace file: {e}")
        logger.error("Please check your internet connection and try again")

        # Clean up partial files on failure
        for partial_file in [COMPRESSED_FILE, TRACE_FILE]:
            if os.path.exists(partial_file):
                try:
                    os.remove(partial_file)
                    logger.info(f"Cleaned up partial file: {partial_file}")
                except OSError:
                    pass

        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to extract trace file: {e}")

        # Clean up partial files on failure
        for partial_file in [COMPRESSED_FILE, TRACE_FILE]:
            if os.path.exists(partial_file):
                try:
                    os.remove(partial_file)
                    logger.info(f"Cleaned up partial file: {partial_file}")
                except OSError:
                    pass

        sys.exit(1)


if __name__ == "__main__":
    logger.info("Starting trace data preparation...")
    download_and_extract_traces()
    logger.info("Data preparation complete!")
