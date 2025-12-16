import json
import urllib.request
from datetime import datetime, timedelta, timezone

EMA_URL = "https://www.ema.europa.eu/en/documents/report/documents-output-json-report_en.json"

def parse_date(s: str):
    if not s:
        return None
    # Try common ISO-like formats
    try:
        # Handles e.g. "2025-12-16T10:00:00Z" or similar
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def main(days: int = 3):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    with urllib.request.urlopen(EMA_URL) as r:
        raw = r.read()
    data = json.loads(raw)

    items = []
    for it in data:
        d = parse_date(it.get("last_update_date")) or parse_date(it.get("publish_date"))
        if d and d >= cutoff:
            items.append(it)

    out = {
        "source": "EMA documents JSON (filtered)",
        "days": days,
        "cutoff_utc": cutoff.isoformat(),
        "count": len(items),
        "items": items,
    }

    with open("filtered_ema.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False)

if __name__ == "__main__":
    main(days=3)
