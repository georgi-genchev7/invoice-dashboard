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
import time
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

SESSION_QUERY = """
WITH FinalThreadStatus AS (
  SELECT
    user_id,
    account_id,
    provider_id,
    provider_name,
    thread_id,
    task_completion_status,
    createdAt AS thread_time
  FROM `product_metrics.browser_use_processing_event`
  WHERE provider_id IS NOT NULL
    AND user_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY thread_id
    ORDER BY
      IF(task_completion_status = 'task_completed', 1, 0) DESC,
      createdAt DESC
  ) = 1
),
-- Pairs of consecutive login events per user × vendor
LoginPairs AS (
  SELECT
    user_id,
    account_id,
    provider_id,
    provider_name,
    thread_time AS login_time,
    LEAD(thread_time) OVER (
      PARTITION BY user_id, provider_id
      ORDER BY thread_time
    ) AS next_login_time
  FROM FinalThreadStatus
  WHERE task_completion_status IN ('login_requested', 'reconnect_requested')
),
-- Only keep pairs where the user actually completed at least one fetch in between,
-- confirming they logged in and the session was real.
-- Active sessions (no subsequent login prompt yet) use CURRENT_TIMESTAMP() as the running end.
ValidSessions AS (
  SELECT
    l.user_id,
    l.account_id,
    l.provider_name,
    (l.next_login_time IS NULL) AS is_active,
    ROUND(
      TIMESTAMP_DIFF(
        COALESCE(l.next_login_time, CURRENT_TIMESTAMP()),
        l.login_time,
        MINUTE
      ) / 60.0,
      1
    ) AS session_hours,
    (
      SELECT COUNT(*)
      FROM FinalThreadStatus s
      WHERE s.user_id    = l.user_id
        AND s.provider_id = l.provider_id
        AND s.thread_time > l.login_time
        AND (l.next_login_time IS NULL OR s.thread_time < l.next_login_time)
    ) AS fetch_attempts,
    (
      SELECT COUNTIF(s.task_completion_status = 'task_completed')
      FROM FinalThreadStatus s
      WHERE s.user_id    = l.user_id
        AND s.provider_id = l.provider_id
        AND s.thread_time > l.login_time
        AND (l.next_login_time IS NULL OR s.thread_time < l.next_login_time)
    ) AS successful_fetches
  FROM LoginPairs l
  WHERE (
    -- Closed session: a subsequent login prompt confirms the session ended;
    -- one task_completed in between is enough to confirm the user actually logged in.
    (l.next_login_time IS NOT NULL AND EXISTS (
      SELECT 1 FROM FinalThreadStatus s
      WHERE s.user_id    = l.user_id
        AND s.provider_id = l.provider_id
        AND s.task_completion_status = 'task_completed'
        AND s.thread_time > l.login_time
        AND s.thread_time < l.next_login_time
    ))
    OR
    -- Active session: no re-login prompt yet.
    -- Require at least one task_completed (confirms login happened) AND
    -- at least 2 total fetch attempts of any kind (confirms ongoing use —
    -- rules out one-off fetches that were never retried, which would show
    -- no re-login only because nobody tried again).
    (l.next_login_time IS NULL
      AND EXISTS (
        SELECT 1 FROM FinalThreadStatus s
        WHERE s.user_id    = l.user_id
          AND s.provider_id = l.provider_id
          AND s.task_completion_status = 'task_completed'
          AND s.thread_time > l.login_time
      )
      AND (
        SELECT COUNT(*) FROM FinalThreadStatus s
        WHERE s.user_id    = l.user_id
          AND s.provider_id = l.provider_id
          AND s.thread_time > l.login_time
      ) >= 2
    )
  )
)
SELECT
  user_id,
  account_id,
  provider_name,
  COUNT(*) AS sessions,
  COUNTIF(is_active) AS active_sessions,
  ROUND(AVG(session_hours), 1) AS avg_hours,
  MIN(session_hours) AS min_hours,
  MAX(session_hours) AS max_hours,
  APPROX_QUANTILES(session_hours, 100)[OFFSET(50)] AS p50_hours,
  ROUND(AVG(fetch_attempts), 1) AS avg_fetch_attempts,
  ROUND(AVG(successful_fetches), 1) AS avg_successful_fetches
FROM ValidSessions
WHERE session_hours > 0
GROUP BY user_id, account_id, provider_name
ORDER BY sessions DESC
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


def fetch_bq_rows(query: str = BQ_QUERY, max_retries: int = 3, backoff: int = 10) -> list[dict]:
    """Run the query via bq CLI with retries on transient failures."""
    cmd = [
        "bq",
        "query",
        "--project_id",
        BQ_PROJECT,
        "--use_legacy_sql=false",
        "--format=json",
        "--max_rows=100000",
        query,
    ]

    for attempt in range(1, max_retries + 1):
        print(
            f"Running BigQuery query against {BQ_PROJECT} (attempt {attempt}/{max_retries}) ..."
        )
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            rows = json.loads(result.stdout)
            print(f"Fetched {len(rows)} rows from BigQuery")
            return rows

        print(f"bq query failed (exit {result.returncode}):", file=sys.stderr)
        print(result.stderr, file=sys.stderr)

        if attempt < max_retries:
            wait = backoff * attempt
            print(f"Retrying in {wait}s ...", file=sys.stderr)
            time.sleep(wait)

    print("All retries exhausted", file=sys.stderr)
    sys.exit(1)


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


def transform_session_rows(rows: list[dict]) -> list[dict]:
    """Convert session gap BQ rows into the compact format the dashboard expects."""
    result = []
    for row in rows:
        try:
            result.append(
                {
                    "uid": row["user_id"],
                    "aid": row["account_id"],
                    "pn": row["provider_name"],
                    "n": int(row["sessions"]),
                    "active": int(row["active_sessions"]) > 0,
                    "avg": round(float(row["avg_hours"]), 1),
                    "p50": round(float(row["p50_hours"]), 1),
                    "min": round(float(row["min_hours"]), 1),
                    "max": round(float(row["max_hours"]), 1),
                    "avg_attempts": round(float(row["avg_fetch_attempts"]), 1),
                    "avg_successes": round(float(row["avg_successful_fetches"]), 1),
                    "seg": classify(row["account_id"]),
                }
            )
        except (ValueError, TypeError, KeyError):
            continue
    return result


# ── Build HTML ──────────────────────────────────────────────────────


def build_html(raw_data: list[dict], session_data: list[dict]) -> str:
    template = TEMPLATE_PATH.read_text()
    html = template.replace("/* __RAW_DATA__ */", json.dumps(raw_data, separators=(",", ":")))
    html = html.replace("/* __SESSION_DATA__ */", json.dumps(session_data, separators=(",", ":")))

    if "/* __RAW_DATA__ */" in html or "/* __SESSION_DATA__ */" in html:
        print(
            "ERROR: placeholder was not replaced — check template.html", file=sys.stderr
        )
        sys.exit(1)

    return html


# ── Main ────────────────────────────────────────────────────────────


def main():
    rows = fetch_bq_rows(BQ_QUERY)
    raw = transform_rows(rows)

    print("Running session length query ...")
    session_rows = fetch_bq_rows(SESSION_QUERY)
    sessions = transform_session_rows(session_rows)
    print(f"Fetched {len(sessions)} session-length pairs")

    html = build_html(raw, sessions)

    OUTPUT_DIR.mkdir(exist_ok=True)
    output_path = OUTPUT_DIR / "agent-fetch-dashboard.html"
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
