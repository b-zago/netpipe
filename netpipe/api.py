import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import click
import requests

# Dev mode enables LocalStack-friendly behavior: HTTP endpoints pass the
# HTTPS enforcement check, and presigned URL hosts that come back as raw
# container IPs get rewritten to localhost. Default is False (prod-safe).
DEV = os.environ.get("NETPIPE_DEV", "").lower() in ("1", "true", "yes")


class _Progress:
    """Minimal progress bar tuned for VHS.

    click.progressbar auto-sizes to the terminal, which under VHS can exceed
    the actual render width and wrap — after which \\r returns to the start of
    the wrapped line, not the bar, so each update looks like a new line. This
    writes a fixed-width line (~55 chars) to stdout with \\x1b[2K\\r so VHS
    always has enough room and repaints cleanly.
    """
    _BAR_WIDTH = 30

    def __init__(self, length, label):
        self.length = length
        self.label = label
        self.pos = 0
        self._lock = threading.Lock()
        self._render()

    def update(self, n):
        with self._lock:
            self.pos = max(0, min(self.pos + n, self.length))
            self._render()

    def _render(self):
        pct = (self.pos / self.length) if self.length else 0
        filled = int(self._BAR_WIDTH * pct)
        bar = "=" * filled + " " * (self._BAR_WIDTH - filled)
        mb_done = self.pos / (1024 * 1024)
        mb_total = self.length / (1024 * 1024)
        sys.stdout.write(f"\x1b[2K\r{self.label} [{bar}] {mb_done:5.1f}/{mb_total:.1f} MB")
        sys.stdout.flush()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        sys.stdout.write("\n")
        sys.stdout.flush()


PART_SIZE = 10 * 1024 * 1024
MAX_WORKERS = 8
MAX_RETRIES = 5
RETRY_BASE = 2.0
BUFFER_SIZE = 1024 * 1024

# (connect, read) timeouts. The read timeout is the max idle gap between bytes,
# not a total-transfer deadline, so 300s on transfers means "server stopped
# sending for 5 minutes" — safe for slow part uploads/downloads.
META_TIMEOUT = (10, 30)
TRANSFER_TIMEOUT = (15, 60)


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


def _request_upload(api_base, access_key, folder, filename, size, multipart):
    resp = requests.put(
        f"{api_base}/send",
        headers={
            "folder-name": folder,
            "file-name": filename,
            "file-size": str(size),
            "multipart": "true" if multipart else "false",
            "authorization": access_key,
        },
        timeout=META_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    if multipart:
        data["parts"] = [{**p, "url": _fix_host(p["url"])} for p in data["parts"]]
    else:
        data["url"] = _fix_host(data["url"])
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
                r = requests.put(
                    url,
                    data=body,
                    headers={"Content-Length": str(size)},
                    timeout=TRANSFER_TIMEOUT,
                )
                r.raise_for_status()
            return {"part_number": part_number, "etag": r.headers["ETag"]}
        except (requests.RequestException, OSError) as e:
            last_err = e
            report(-sent[0])
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt)
    raise last_err


def _upload_multipart(api_base, access_key, folder, filename, local_path, file_size, workers):
    info = _request_upload(api_base, access_key, folder, filename, file_size, multipart=True)
    upload_id = info["upload_id"]
    parts = info["parts"]

    completed = []

    with _Progress(file_size, filename) as bar:
        def report(n):
            bar.update(n)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_upload_part, p["url"], local_path, p["part_number"], file_size, report)
                for p in parts
            ]
            for fut in as_completed(futures):
                try:
                    completed.append(fut.result())
                except Exception as e:
                    click.echo(f"\npart failed: {e}", err=True)
                    raise

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


class _MultipartStream:
    """Iterable body for a presigned-POST upload: preamble → file → epilogue.

    Exposes `.len` so `requests` uses Content-Length instead of chunked encoding
    (S3 rejects chunked encoding on presigned POSTs). Iteration yields 1 MB
    chunks directly — going through a `read()`-based file-like instead drops
    throughput roughly in half, because urllib3 pulls in 16 KB chunks and the
    per-call Python/sendall overhead dominates on a 100 MB upload.
    """

    CHUNK = 1024 * 1024

    def __init__(self, preamble: bytes, local_path: str, file_size: int,
                 epilogue: bytes, on_progress) -> None:
        self._preamble = preamble
        self._epilogue = epilogue
        self._local_path = local_path
        self._file_size = file_size
        self._on_progress = on_progress
        self.len = len(preamble) + file_size + len(epilogue)

    def __iter__(self):
        yield self._preamble
        remaining = self._file_size
        with open(self._local_path, "rb") as f:
            while remaining > 0:
                chunk = f.read(min(self.CHUNK, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                self._on_progress(len(chunk))
                yield chunk
        yield self._epilogue


def _post_single(url, fields, filename, local_path, file_size, on_progress):
    boundary = uuid.uuid4().hex

    preamble_parts = []
    for name, value in fields.items():
        preamble_parts.append(f"--{boundary}\r\n".encode())
        preamble_parts.append(
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        )
        preamble_parts.append(f"{value}\r\n".encode())
    preamble_parts.append(f"--{boundary}\r\n".encode())
    preamble_parts.append(
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
    )
    preamble_parts.append(b"Content-Type: application/octet-stream\r\n\r\n")
    preamble = b"".join(preamble_parts)
    epilogue = f"\r\n--{boundary}--\r\n".encode()

    body = _MultipartStream(preamble, local_path, file_size, epilogue, on_progress)
    r = requests.post(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        timeout=TRANSFER_TIMEOUT,
    )
    r.raise_for_status()


def _upload_single(api_base, access_key, folder, filename, local_path, file_size):
    info = _request_upload(api_base, access_key, folder, filename, file_size, multipart=False)
    url = info["url"]
    fields = info["fields"]

    with _Progress(file_size, filename) as bar:
        last_err = None
        for attempt in range(MAX_RETRIES):
            sent = [0]

            def on_progress(n, sent=sent):
                sent[0] += n
                bar.update(n)

            try:
                _post_single(url, fields, filename, local_path, file_size, on_progress)
                return
            except (requests.RequestException, OSError) as e:
                last_err = e
                bar.update(-sent[0])
                if attempt < MAX_RETRIES - 1:
                    _backoff(attempt)
        raise last_err


def upload_file(api_base: str, access_key: str, folder: str, local_path: str,
                multipart: bool = False, workers: int = MAX_WORKERS) -> None:
    file_size = os.path.getsize(local_path)
    filename = os.path.basename(local_path)

    if multipart:
        _upload_multipart(api_base, access_key, folder, filename, local_path, file_size, workers)
    else:
        _upload_single(api_base, access_key, folder, filename, local_path, file_size)


def _request_download(api_base, access_key, folder, filename, multipart):
    resp = requests.get(
        f"{api_base}/file",
        headers={
            "folder-name": folder,
            "file-name": filename,
            "multipart": "true" if multipart else "false",
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
    if multipart:
        data["parts"] = [{**p, "url": _fix_host(p["url"])} for p in data["parts"]]
    else:
        data["url"] = _fix_host(data["url"])
    return data


def _download_part_to(out_path: str, url: str, part_number: int, start: int, end: int) -> int:
    """Download one part directly into `out_path` at byte offset `start`."""
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


def _download_multipart(data: dict, local_path: str, workers: int) -> None:
    parts = data["parts"]
    file_size = data["file_size"]
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
    part_sizes = {p["part_number"]: p["end"] - p["start"] + 1 for p in parts}

    label = os.path.basename(local_path)
    with _Progress(file_size, label) as bar:
        bar.update(sum(part_sizes[pn] for pn in done))

        with ThreadPoolExecutor(max_workers=workers) as ex:
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
                bar.update(part_sizes[part_number])

    if os.path.exists(progress_path):
        os.remove(progress_path)


def _download_single(data: dict, local_path: str) -> None:
    url = data["url"]
    file_size = data["file_size"]

    label = os.path.basename(local_path)
    with _Progress(file_size, label) as bar:
        last_err = None
        for attempt in range(MAX_RETRIES):
            written = 0
            try:
                with requests.get(url, stream=True, timeout=TRANSFER_TIMEOUT) as r:
                    r.raise_for_status()
                    with open(local_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=BUFFER_SIZE):
                            if chunk:
                                f.write(chunk)
                                written += len(chunk)
                                bar.update(len(chunk))
                if written != file_size:
                    raise RuntimeError(
                        f"size mismatch: expected {file_size}, got {written}"
                    )
                return
            except (requests.RequestException, OSError, RuntimeError) as e:
                last_err = e
                bar.update(-written)
                if attempt < MAX_RETRIES - 1:
                    _backoff(attempt)
        raise last_err


def download_file(api_base: str, access_key: str, folder: str, filename: str, local_path: str,
                  multipart: bool = False, workers: int = MAX_WORKERS) -> None:
    data = _request_download(api_base, access_key, folder, filename, multipart)
    if multipart:
        _download_multipart(data, local_path, workers)
    else:
        _download_single(data, local_path)


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
