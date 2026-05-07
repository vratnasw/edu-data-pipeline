"""HTTP client with exponential-backoff retry + size/duration logging."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger(__name__)


def get_with_backoff(url: str, params: dict | None = None,
                       headers: dict | None = None,
                       max_retries: int = 4, timeout: int = 60,
                       verify: bool = True) -> requests.Response:
    last: Exception | None = None
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, headers=headers,
                              timeout=timeout, stream=False, verify=verify)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"http {r.status_code} on {url}")
                wait = 2 ** attempt
                log.warning("retrying %s in %ds (got %d)", url, wait, r.status_code)
                time.sleep(wait); continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            wait = 2 ** attempt
            log.warning("retry %s in %ds (%s)", url, wait, e)
            time.sleep(wait)
    raise RuntimeError(f"failed after {max_retries} attempts: {url} :: {last}")


def head_last_modified(url: str) -> Optional[str]:
    """Return the Last-Modified header (or None)."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=20)
        return r.headers.get("Last-Modified") or r.headers.get("last-modified")
    except Exception:  # noqa: BLE001
        return None


def download_to(url: str, dest: Path, params: dict | None = None,
                 headers: dict | None = None, verify: bool = True) -> dict:
    """Stream-download a file. Returns {bytes, duration_s}."""
    dest = Path(dest); dest.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with requests.get(url, params=params, headers=headers,
                         stream=True, timeout=300, verify=verify) as r:
        r.raise_for_status()
        total = 0
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk); total += len(chunk)
    dur = time.time() - t0
    log.info("downloaded %s (%.1f MB) -> %s in %.1fs",
              url, total / 1024 / 1024, dest, dur)
    return {"bytes": total, "duration_s": round(dur, 2)}
