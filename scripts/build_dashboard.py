import json
import os
from datetime import datetime, timezone

SAMPLE_DATA = [
    {"provider_name": "Acme Corp", "ATTEMPTS": 1200, "LOGIN_REQUESTED": 60, "SUCCESS": 1050, "CAPTCHA": 30, "FETCH_FAILED": 35, "AGENT_BLOCKED": 25},
    {"provider_name": "Beta Inc", "ATTEMPTS": 870, "LOGIN_REQUESTED": 45, "SUCCESS": 740, "CAPTCHA": 22, "FETCH_FAILED": 40, "AGENT_BLOCKED": 23},
    {"provider_name": "Gamma Ltd", "ATTEMPTS": 650, "LOGIN_REQUESTED": 30, "SUCCESS": 570, "CAPTCHA": 15, "FETCH_FAILED": 20, "AGENT_BLOCKED": 15},
    {"provider_name": "Delta SA", "ATTEMPTS": 430, "LOGIN_REQUESTED": 20, "SUCCESS": 370, "CAPTCHA": 10, "FETCH_FAILED": 18, "AGENT_BLOCKED": 12},
    {"provider_name": "Epsilon GmbH", "ATTEMPTS": 310, "LOGIN_REQUESTED": 18, "SUCCESS": 255, "CAPTCHA": 12, "FETCH_FAILED": 15, "AGENT_BLOCKED": 10},
    {"provider_name": "Zeta Co", "ATTEMPTS": 280, "LOGIN_REQUESTED": 15, "SUCCESS": 230, "CAPTCHA": 8, "FETCH_FAILED": 17, "AGENT_BLOCKED": 10},
    {"provider_name": "Eta Systems", "ATTEMPTS": 195, "LOGIN_REQUESTED": 10, "SUCCESS": 160, "CAPTCHA": 7, "FETCH_FAILED": 10, "AGENT_BLOCKED": 8},
    {"provider_name": "Theta Digital", "ATTEMPTS": 150, "LOGIN_REQUESTED": 8, "SUCCESS": 120, "CAPTCHA": 5, "FETCH_FAILED": 12, "AGENT_BLOCKED": 5},
]


class _DictRow(dict):
    __getattr__ = dict.__getitem__


def main():
    query = os.environ.get("BQ_QUERY", "")
    use_bigquery = query and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if use_bigquery:
        from google.cloud import bigquery
        client = bigquery.Client()
        rows = list(client.query(query).result())
    else:
        print("No BigQuery credentials found — using sample data")
        rows = [_DictRow(r) for r in SAMPLE_DATA]

    providers = []
    totals = {"a": 0, "s": 0, "l": 0, "ca": 0, "f": 0, "b": 0}

    for row in rows:
        attempts = row.ATTEMPTS or 0
        success = row.SUCCESS or 0
        login = row.LOGIN_REQUESTED or 0
        captcha = row.CAPTCHA or 0
        fetch_failed = row.FETCH_FAILED or 0
        blocked = row.AGENT_BLOCKED or 0

        pct = lambda v: round(v / attempts * 100, 2) if attempts else 0

        providers.append({
            "n": row.provider_name,
            "a": attempts,
            "s": pct(success),
            "l": pct(login),
            "ca": pct(captcha),
            "f": pct(fetch_failed),
            "b": pct(blocked),
        })

        totals["a"] += attempts
        totals["s"] += success
        totals["l"] += login
        totals["ca"] += captcha
        totals["f"] += fetch_failed
        totals["b"] += blocked

    total_a = totals["a"] or 1
    stats = {
        "total": totals["a"],
        "success": round(totals["s"] / total_a * 100, 2),
        "login": round(totals["l"] / total_a * 100, 2),
        "captcha": round(totals["ca"] / total_a * 100, 2),
        "fetch_failed": round(totals["f"] / total_a * 100, 2),
        "blocked": round(totals["b"] / total_a * 100, 2),
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    with open("template.html", "r") as f:
        html = f.read()

    injection = f"const D={json.dumps(providers)};const S={json.dumps(stats)};"
    html = html.replace("/* __DATA__ */", injection)

    os.makedirs("output", exist_ok=True)
    with open("output/index.html", "w") as f:
        f.write(html)

    print(f"Dashboard built: {len(providers)} providers, {stats['total']} total attempts")


if __name__ == "__main__":
    main()
