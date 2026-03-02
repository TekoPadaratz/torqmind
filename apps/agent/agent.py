import argparse
import json
import gzip
import io
import requests

"""Tiny helper to test /ingest with NDJSON.

Usage:
  python agent.py --api http://localhost:8000 --dataset comprovantes --ingest-key <uuid>

If --ingest-key is omitted, API will fallback to tenant=1 only when INGEST_REQUIRE_KEY=False (dev).
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", required=True, help="Base da API, ex: http://localhost:8000")
    ap.add_argument("--dataset", default="movprodutos")
    ap.add_argument("--ingest-key", default=None)
    args = ap.parse_args()

    # Minimal example row (adapt for your dataset)
    rows = [
        {
            "ID_FILIAL": 1,
            "ID_DB": 1,
            "ID_MOVPRODUTOS": 123,
            "DATA": "2026-01-01T10:00:00",
            "TOTALVENDA": 100.0,
        }
    ]

    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows).encode("utf-8")

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(ndjson)

    headers = {
        "Content-Type": "application/x-ndjson",
        "Content-Encoding": "gzip",
    }
    if args.ingest_key:
        headers["X-Ingest-Key"] = args.ingest_key

    url = f"{args.api.rstrip('/')}/ingest/{args.dataset}"
    r = requests.post(url, data=buf.getvalue(), headers=headers, timeout=60)
    print(r.status_code)
    print(r.text[:2000])


if __name__ == "__main__":
    main()
