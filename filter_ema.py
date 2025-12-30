import json
import urllib.request
import http.client
import gzip
import io
from datetime import datetime, timedelta, timezone

EMA_URL = "https://www.ema.europa.eu/en/documents/report/documents-output-json-report_en.json"

def parse_date(s: str):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "data", "records", "results"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []

def fetch_bytes(url: str, retries: int = 3, timeout: int = 120) -> bytes:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept-Encoding": "gzip",
                    "Accept": "application/json,text/plain,*/*",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                chunks = []
                while True:
                    try:
                        chunk = r.read(1024 * 1024)  # 1 MB
                    except http.client.IncompleteRead as e:
                        # keep whatever was received and stop
                        if e.partial:
                            chunks.append(e.partial)
                        break
                    if not chunk:
                        break
                    chunks.append(chunk)

                data = b"".join(chunks)

                enc = (r.headers.get("Content-Encoding") or "").lower()
                if "gzip" in enc:
                    data = gzip.decompress(data)

                return data

        except Exception as e:
            last_err = e

    raise last_err

def main(days: int = 3):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    raw = fetch_bytes(EMA_URL, retries=3, timeout=180)
    payload = json.loads(raw.decode("utf-8", errors="replace"))

    records = extract_records(payload)

    items = []
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

    with open("docs/filtered_ema.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

    print(f"OK records={len(records)} filtered={len(items)} skipped_str={skipped_str} skipped_other={skipped_other}")

if __name__ == "__main__":
    # Change this number to test (e.g., 30) then set back to 3 later.
    main(days=30)
