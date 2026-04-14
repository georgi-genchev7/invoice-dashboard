"""
Build the Agent Fetch Performance dashboard.

Fetches data from BigQuery, classifies rows by segment, and injects
the RAW data array into the HTML template for GitHub Pages deployment.

Usage:
  # With BigQuery (CI or local with gcloud auth):
  python scripts/build_dashboard.py

  # Override project:
  BQ_PROJECT=data-warehouse-349811 python scripts/build_dashboard.py
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
TEMPLATE_PATH = REPO_ROOT / "template.html"
OUTPUT_DIR = REPO_ROOT / "output"

BQ_PROJECT = os.environ.get("BQ_PROJECT", "data-warehouse-349811")

BQ_QUERY = """
WITH FinalThreadStatus AS (
  SELECT
    account_id,
    provider_name,
    thread_id,
    task_completion_status,
    initiator,
    DATE(createdAt) AS attempt_date
  FROM `product_metrics.browser_use_processing_event`
  WHERE provider_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY thread_id
    ORDER BY
      IF(task_completion_status = 'task_completed', 1, 0) DESC,
      createdAt DESC
  ) = 1
)

SELECT
  account_id,
  provider_name,
  attempt_date,
  initiator,
  COUNT(thread_id) AS ATTEMPTS,
  ROUND(COUNTIF(task_completion_status = 'login_requested' OR task_completion_status = 'reconnect_requested') / COUNT(thread_id) * 100, 2) AS LOGIN_REQUESTED,
  ROUND(COUNTIF(task_completion_status = 'task_completed') / COUNT(thread_id) * 100, 2) AS SUCCESS,
  ROUND(COUNTIF(task_completion_status = 'captcha_requested') / COUNT(thread_id) * 100, 2) AS CAPTCHA,
  ROUND(COUNTIF(task_completion_status = 'task_failed') / COUNT(thread_id) * 100, 2) AS FETCH_FAILED,
  ROUND(COUNTIF(task_completion_status = 'url_blocked') / COUNT(thread_id) * 100, 2) AS AGENT_BLOCKED
FROM FinalThreadStatus
GROUP BY
  account_id,
  provider_name,
  attempt_date,
  initiator
ORDER BY
  SUCCESS DESC, FETCH_FAILED DESC, ATTEMPTS DESC
""".strip()

# ── Segment classification ──────────────────────────────────────────

INTERNAL_ACCOUNTS = {
    "robbie_demo_19c0be1e_demo",
    "payhawk_bulgaria_demo_ec0ec516_demo",
    "georgi_genchev_eur_bc05d24a_demo",
    "georgi_genchev_usd_aaaf7ffe_demo",
}


def classify(account_id: str) -> str:
    aid = account_id.lower()
    if account_id in INTERNAL_ACCOUNTS or "robbie" in aid:
        return "internal"
    if "payhawk" in aid:
        return "payhawk_live"
    return "customer_live"


# ── BigQuery fetch ──────────────────────────────────────────────────


def fetch_bq_rows() -> list[dict]:
    """Run the query via bq CLI (works with gcloud auth and SA keys)."""
    cmd = [
        "bq",
        "query",
        "--project_id",
        BQ_PROJECT,
        "--use_legacy_sql=false",
        "--format=json",
        "--max_rows=100000",
        BQ_QUERY,
    ]
    print(f"Running BigQuery query against {BQ_PROJECT} ...")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"bq query failed (exit {result.returncode}):", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    rows = json.loads(result.stdout)
    print(f"Fetched {len(rows)} rows from BigQuery")
    return rows


# ── Transform rows to RAW format ────────────────────────────────────


def transform_rows(rows: list[dict]) -> list[dict]:
    """Convert BQ percentage rows into the compact RAW format the dashboard expects."""
    raw = []
    for row in rows:
        a = int(row["ATTEMPTS"])
        raw.append(
            {
                "aid": row["account_id"],
                "pn": row["provider_name"],
                "dt": row["attempt_date"],
                "ini": row["initiator"],
                "a": a,
                "s": round(float(row["SUCCESS"]) * a / 100),
                "l": round(float(row["LOGIN_REQUESTED"]) * a / 100),
                "ca": round(float(row["CAPTCHA"]) * a / 100),
                "f": round(float(row["FETCH_FAILED"]) * a / 100),
                "b": round(float(row["AGENT_BLOCKED"]) * a / 100),
                "seg": classify(row["account_id"]),
            }
        )
    return raw


# ── Build HTML ──────────────────────────────────────────────────────


def build_html(raw_data: list[dict]) -> str:
    template = TEMPLATE_PATH.read_text()
    data_js = json.dumps(raw_data, separators=(",", ":"))
    html = template.replace("/* __RAW_DATA__ */", data_js)

    if "/* __RAW_DATA__ */" in html:
        print(
            "ERROR: placeholder was not replaced — check template.html", file=sys.stderr
        )
        sys.exit(1)

    return html


# ── Main ────────────────────────────────────────────────────────────


def main():
    rows = fetch_bq_rows()
    raw = transform_rows(rows)

    html = build_html(raw)

    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / "index.html"
    output_path.write_text(html)

    # Stats summary
    total = sum(r["a"] for r in raw)
    success = sum(r["s"] for r in raw)
    pct = round(success / total * 100, 1) if total else 0
    segments = {}
    for r in raw:
        segments[r["seg"]] = segments.get(r["seg"], 0) + r["a"]

    print(f"\nDashboard built: {output_path}")
    print(f"  Rows: {len(raw)}")
    print(f"  Total attempts: {total}")
    print(f"  Overall success: {pct}%")
    print(f"  Segments: {segments}")


if __name__ == "__main__":
    main()
