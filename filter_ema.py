import json
import sys
import time
import gzip
import http.client
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

EMA_URL = "https://www.ema.europa.eu/en/documents/report/documents-output-json-report_en.json"

def parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def extract_records(payload: Any) -> List[Any]:
    # EMA payload may be a list OR a dict wrapping a list
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "data", "records", "results"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # fallback: first list value
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []

def fetch_bytes(url: str, attempts: int = 5, timeout: int = 120) -> bytes:
    last_err = None
    for i in range(1, attempts + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Encoding": "gzip",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                encoding = (resp.headers.get("Content-Encoding") or "").lower()
                buf = bytearray()
                try:
                    while True:
                        chunk = resp.read(1024 * 1024)  # 1 MB chunks
                        if not chunk:
                            break
                        buf.extend(chunk)
                except http.client.IncompleteRead as e:
                    # got partial data; treat as failure so we retry
                    buf.extend(e.partial or b"")
                    raise

            data = bytes(buf)
            if encoding == "gzip":
                data = gzip.decompress(data)

            # Sanity check: avoid parsing HTML error pages as JSON
            head = data.lstrip()[:1]
            if head not in (b"{", b"["):
                preview = data.lstrip()[:300].decode("utf-8", errors="replace")
                raise ValueError(f"Response is not JSON (starts with {head!r}). Preview:\n{preview}")

            return data

        except Exception as e:
            last_err = e
            # backoff retry
            time.sleep(min(2 ** (i - 1), 10))

    raise RuntimeError(f"Failed to download a valid JSON payload after {attempts} attempts: {last_err}")

def main(days: int = 3) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    raw = fetch_bytes(EMA_URL, attempts=5, timeout=180)

    try:
        payload = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as e:
        # print a small tail/head for debugging
        txt = raw.decode("utf-8", errors="replace")
        print("JSON decode failed. First 500 chars:\n", txt[:500], file=sys.stderr)
        print("\nLast 500 chars:\n", txt[-500:], file=sys.stderr)
        raise

    records = extract_records(payload)

    items: List[Dict[str, Any]] = []
    skipped_str = 0
    skipped_other = 0

    for it in records:
        if isinstance(it, str):
            skipped_str += 1
            s = it.strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    it = json.loads(s)
                except Exception:
                    continue
            else:
                continue

        if not isinstance(it, dict):
            skipped_other += 1
            continue

        d = parse_date(it.get("last_update_date")) or parse_date(it.get("publish_date"))
        if d and d >= cutoff:
            items.append(it)

    out = {
        "source": "EMA documents JSON (filtered)",
        "days": days,
        "cutoff_utc": cutoff.isoformat(),
        "count": len(items),
        "debug_skipped_str": skipped_str,
        "debug_skipped_other": skipped_other,
        "items": items,
    }

    # Write pretty JSON (easier to inspect) but still valid
    with open("docs/filtered_ema.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"OK records={len(records)} filtered={len(items)} skipped_str={skipped_str} skipped_other={skipped_other}")

if __name__ == "__main__":
    main(days=15)
