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
    """
    EMA payload can be:
      - a list of records
      - a dict containing the list under some key (e.g., 'data', 'items', etc.)
    We return a list of dict-like records.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "data", "records", "results"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # fallback: if dict values contain a list, use the first list we find
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
    for it in records:
        try:
            last = it.get("last_update_date")
            pub = it.get("publish_date")
        except Exception:
            continue  # skips strings or anything non-dict safely

        d = parse_date(last) or parse_date(pub)
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
