import json
import urllib.request
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

def main(days: int = 3):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    req = urllib.request.Request(
        EMA_URL,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    with urllib.request.urlopen(req) as r:
        raw = r.read()

    payload = json.loads(raw)
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
    main(days=30)
