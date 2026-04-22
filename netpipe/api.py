import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import requests

# Dev mode enables LocalStack-friendly behavior: HTTP endpoints pass the
# HTTPS enforcement check, and presigned URL hosts that come back as raw
# container IPs get rewritten to localhost. Default is False (prod-safe).
DEV = os.environ.get("NETPIPE_DEV", "").lower() in ("1", "true", "yes")
PART_SIZE = 100 * 1024 * 1024
MAX_WORKERS = 8
MAX_RETRIES = 3
RETRY_BASE = 1.0
BUFFER_SIZE = 1024 * 1024

# (connect, read) timeouts. The read timeout is the max idle gap between bytes,
# not a total-transfer deadline, so 300s on transfers means "server stopped
# sending for 5 minutes" — safe for slow part uploads/downloads.
META_TIMEOUT = (10, 30)
TRANSFER_TIMEOUT = (10, 300)


def _fix_host(url: str) -> str:
    return re.sub(r"://\d+\.\d+\.\d+\.\d+", "://localhost", url) if DEV else url


def _part_bounds(part_number: int, file_size: int) -> tuple[int, int]:
    """(start, size) for a 1-indexed part. Last part may be smaller."""
    start = (part_number - 1) * PART_SIZE
    return start, min(PART_SIZE, file_size - start)


def _backoff(attempt: int) -> None:
    time.sleep(RETRY_BASE * (2 ** attempt))


class _ProgressFile:
    """File-like view over a slice of a file that reports bytes as they're read.

    Exposes `.len` so `requests` sets Content-Length and avoids chunked encoding
    (which some S3-compatible backends reject on presigned part uploads).
    Each instance owns its own fd, so it is safe to use from worker threads.
    """

    def __init__(self, path: str, start: int, size: int, on_read):
        self._f = open(path, "rb")
        self._f.seek(start)
        self._remaining = size
        self.len = size
        self._on_read = on_read

    def read(self, n=-1):
        if self._remaining <= 0:
            return b""
        if n < 0 or n > self._remaining:
            n = self._remaining
        chunk = self._f.read(n)
        self._remaining -= len(chunk)
        self._on_read(len(chunk))
        return chunk

    def close(self):
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def initiate_upload(api_base: str, access_key: str, folder: str, filename: str, size: int) -> dict:
    resp = requests.put(
        f"{api_base}/send",
        headers={
            "folder-name": folder,
            "file-name": filename,
            "file-size": str(size),
            "authorization": access_key,
        },
        timeout=META_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    data["parts"] = [{**p, "url": _fix_host(p["url"])} for p in data["parts"]]
    return data


def _upload_part(url: str, local_path: str, part_number: int, file_size: int, report) -> dict:
    start, size = _part_bounds(part_number, file_size)

    last_err = None
    for attempt in range(MAX_RETRIES):
        sent = [0]

        def on_read(n, sent=sent):
            sent[0] += n
            report(n)

        try:
            with _ProgressFile(local_path, start, size, on_read) as body:
                r = requests.put(url, data=body, timeout=TRANSFER_TIMEOUT)
                r.raise_for_status()
            return {"part_number": part_number, "etag": r.headers["ETag"]}
        except (requests.RequestException, OSError) as e:
            last_err = e
            report(-sent[0])  # roll back progress so the retry doesn't double-count
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
    raise last_err


def upload_file(api_base: str, access_key: str, folder: str, local_path: str) -> None:
    file_size = os.path.getsize(local_path)
    filename = os.path.basename(local_path)

    info = initiate_upload(api_base, access_key, folder, filename, file_size)
    upload_id = info["upload_id"]
    parts = info["parts"]

    completed = []
    bar_lock = threading.Lock()

    with click.progressbar(length=file_size, label=filename, show_pos=True) as bar:
        def report(n):
            with bar_lock:
                bar.update(n)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [
                ex.submit(_upload_part, p["url"], local_path, p["part_number"], file_size, report)
                for p in parts
            ]
            for fut in as_completed(futures):
                completed.append(fut.result())

    resp = requests.post(
        f"{api_base}/complete",
        headers={
            "folder-name": folder,
            "file-name": filename,
            "authorization": access_key,
        },
        json={
            "upload_id": upload_id,
            "parts": sorted(completed, key=lambda p: p["part_number"]),
        },
        timeout=META_TIMEOUT,
    )
    resp.raise_for_status()


def get_download_parts(api_base: str, access_key: str, folder: str, filename: str) -> dict:
    resp = requests.get(
        f"{api_base}/file",
        headers={
            "folder-name": folder,
            "file-name": filename,
            "authorization": access_key,
        },
        timeout=META_TIMEOUT,
    )
    if resp.status_code == 413:
        raise RuntimeError(f"quota exceeded: {resp.json().get('message')}")
    if resp.status_code == 404:
        raise FileNotFoundError(resp.json().get("message"))
    resp.raise_for_status()
    data = resp.json()
    data["parts"] = [{**p, "url": _fix_host(p["url"])} for p in data["parts"]]
    return data


def _download_part_to(out_path: str, url: str, part_number: int, start: int, end: int) -> int:
    """Download one part directly into `out_path` at byte offset `start`.

    Returns the part_number on success so the caller can record it to the sidecar.
    The file must already exist at full size (pre-allocated by the caller).
    """
    expected_size = end - start + 1
    headers = {"Range": f"bytes={start}-{end}"}

    last_err = None
    for attempt in range(MAX_RETRIES):
        written = 0
        try:
            with requests.get(url, headers=headers, stream=True, timeout=TRANSFER_TIMEOUT) as r:
                r.raise_for_status()
                with open(out_path, "r+b") as f:
                    f.seek(start)
                    for chunk in r.iter_content(chunk_size=BUFFER_SIZE):
                        if chunk:
                            f.write(chunk)
                            written += len(chunk)
            if written != expected_size:
                raise RuntimeError(
                    f"part {part_number} size mismatch: expected {expected_size}, got {written}"
                )
            return part_number
        except (requests.RequestException, OSError, RuntimeError) as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
    raise last_err


def download_file(data: dict, local_path: str) -> None:
    parts = data["parts"]
    file_size = data["file_size"]
    total_parts = len(parts)
    progress_path = f"{local_path}.parts"

    # Cross-run resume: sidecar lists part numbers already written. We only
    # trust it if the output file still exists at the expected full size.
    done: set[int] = set()
    if (
        os.path.exists(local_path)
        and os.path.getsize(local_path) == file_size
        and os.path.exists(progress_path)
    ):
        with open(progress_path, "r") as f:
            done = {int(line) for line in f if line.strip()}
    else:
        # Fresh start: pre-allocate the output file at full size so workers can
        # write at their byte offsets in parallel. `truncate` is O(1) on ext4
        # (sparse) and on NTFS (extends logical size; zero-fill happens lazily
        # as writes cross valid-data-length).
        with open(local_path, "wb") as f:
            f.truncate(file_size)
        if os.path.exists(progress_path):
            os.remove(progress_path)

    pending = [p for p in parts if p["part_number"] not in done]
    sidecar_lock = threading.Lock()

    label = f"{os.path.basename(local_path)} ({file_size / (1024*1024):.1f} MB)"
    with click.progressbar(length=total_parts, label=label) as bar:
        bar.update(len(done))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [
                ex.submit(
                    _download_part_to,
                    local_path,
                    p["url"],
                    p["part_number"],
                    p["start"],
                    p["end"],
                )
                for p in pending
            ]
            for fut in as_completed(futures):
                part_number = fut.result()
                with sidecar_lock:
                    with open(progress_path, "a") as pf:
                        pf.write(f"{part_number}\n")
                bar.update(1)

    # Full success: drop the sidecar.
    if os.path.exists(progress_path):
        os.remove(progress_path)


def list_files(api_base: str, access_key: str, folder: str) -> list[dict]:
    resp = requests.get(
        f"{api_base}/list",
        headers={
            "folder-name": folder,
            "authorization": access_key,
        },
        timeout=META_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["files"]
