import json
from decimal import Decimal
from datetime import datetime, timedelta

import requests

from local_ecostok2_config import BATCH_SIZE, get_supabase_key, get_supabase_url


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def build_headers() -> dict:
    key = get_supabase_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def fetch_product_weights() -> list[dict]:
    url = get_supabase_url()
    headers = build_headers()
    table_name = "ecostok2_product_weights"
    response = requests.get(
        f"{url}/rest/v1/{table_name}?select=*",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def count_rows_for_date(table_name: str, date_field: str, report_date: str) -> int:
    url = get_supabase_url()
    headers = build_headers()
    response = requests.get(
        f"{url}/rest/v1/{table_name}?{date_field}=eq.{report_date}&select={date_field}",
        headers={**headers, "Prefer": "count=exact"},
        timeout=30,
    )
    response.raise_for_status()
    content_range = response.headers.get("content-range", "")
    if "/" not in content_range:
        return 0
    total = content_range.split("/")[-1]
    return int(total) if total.isdigit() else 0


def delete_rows_for_date(
    table_name: str,
    date_field: str,
    report_date: str,
    *,
    dry_run: bool = True,
) -> dict:
    summary = {
        "table_name": table_name,
        "date_field": date_field,
        "report_date": report_date,
        "dry_run": dry_run,
        "deleted": 0,
    }

    if dry_run:
        return summary

    url = get_supabase_url()
    headers = build_headers()
    response = requests.delete(
        f"{url}/rest/v1/{table_name}?{date_field}=eq.{report_date}",
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    summary["deleted"] = count_rows_for_date(table_name, date_field, report_date)
    return summary


def insert_rows(
    table_name: str,
    rows: list[dict],
    *,
    dry_run: bool = True,
    batch_size: int = BATCH_SIZE,
) -> dict:
    summary = {
        "table_name": table_name,
        "rows_total": len(rows),
        "batches_total": (len(rows) + batch_size - 1) // batch_size if rows else 0,
        "inserted": 0,
        "errors": 0,
        "dry_run": dry_run,
        "error_details": [],
    }

    if dry_run or not rows:
        return summary

    url = get_supabase_url()
    headers = build_headers()

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        response = requests.post(
            f"{url}/rest/v1/{table_name}",
            headers=headers,
            data=json.dumps(batch, cls=DecimalEncoder),
            timeout=60,
        )
        if response.status_code in (200, 201):
            summary["inserted"] += len(batch)
        else:
            summary["errors"] += 1
            summary["error_details"].append(
                {
                    "status_code": response.status_code,
                    "response_text": response.text[:2000],
                    "payload_sample": batch[:2],
                }
            )

    return summary


def upsert_rows(
    table_name: str,
    rows: list[dict],
    *,
    on_conflict: str,
    dry_run: bool = True,
    batch_size: int = BATCH_SIZE,
) -> dict:
    summary = {
        "table_name": table_name,
        "rows_total": len(rows),
        "batches_total": (len(rows) + batch_size - 1) // batch_size if rows else 0,
        "inserted": 0,
        "errors": 0,
        "dry_run": dry_run,
        "mode": "upsert",
    }

    if dry_run or not rows:
        return summary

    url = get_supabase_url()
    headers = {
        **build_headers(),
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        response = requests.post(
            f"{url}/rest/v1/{table_name}?on_conflict={on_conflict}",
            headers=headers,
            data=json.dumps(batch, cls=DecimalEncoder),
            timeout=60,
        )
        if response.status_code in (200, 201):
            summary["inserted"] += len(batch)
        else:
            summary["errors"] += 1

    return summary


def fetch_rows_for_day(
    table_name: str,
    date_field: str,
    report_date: str,
    *,
    select_fields: str = "*",
    exact_date: bool = False,
) -> list[dict]:
    url = get_supabase_url()
    headers = build_headers()
    rows = []
    offset = 0
    limit = 1000
    next_date = (datetime.strptime(report_date, "%Y-%m-%d").date() + timedelta(days=1)).strftime("%Y-%m-%d")

    while True:
        if exact_date:
            query = f"{url}/rest/v1/{table_name}?{date_field}=eq.{report_date}&select={select_fields}"
        else:
            query = f"{url}/rest/v1/{table_name}?{date_field}=gte.{report_date}T00:00:00&{date_field}=lt.{next_date}T00:00:00&select={select_fields}"
        response = requests.get(
            query,
            headers={**headers, "Range-Unit": "items", "Range": f"{offset}-{offset + limit - 1}"},
            timeout=60,
        )
        response.raise_for_status()
        batch = response.json()
        rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit

    return rows


def sample_rows(rows: list[dict], limit: int = 5) -> list[dict]:
    return rows[:limit]


def refresh_dm_tables(*, dry_run: bool = True) -> dict:
    return {
        "dry_run": dry_run,
        "status": "planned_only",
        "message": "DM refresh intentionally not implemented in MVP.",
    }
