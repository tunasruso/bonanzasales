#!/usr/bin/env python3
import argparse
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

import psycopg2

from local_ecostok2_config import (
    DATE_FIELDS,
    DEFAULT_DAYS_BACK,
    DRY_RUN_LIMIT,
    get_db_config,
    get_target_table,
)
from local_ecostok2_supabase import (
    count_rows_for_date,
    delete_rows_for_date,
    fetch_product_weights,
    fetch_rows_for_day,
    insert_rows,
    sample_rows,
    upsert_rows,
)


WAREHOUSE_REF = "_Fld53725RRef"
NOMENCLATURE_REF = "_Fld53716RRef"
REVENUE_COL = "_Fld53732"
QUANTITY_COL = "_Fld53731"
RECORDER_REF = "_RecorderRRef"
EXCLUDED_RECORDER_TREF_HEX = "000003f1"

COUNTER_DEVICE_REF = "_Fld53555RRef"
VISITOR_COUNT_COL = "_Fld53558"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("local_ecostok2_sync")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["sales", "inventory", "visitors"], required=True)
    parser.add_argument("--date", help="Report date in YYYY-MM-DD format")
    parser.add_argument("--date-to", help="Window end date in YYYY-MM-DD format")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def log_sql(name: str, query: str, params=None) -> None:
    log.info("[SQL][%s]\n%s\nparams=%s", name, query.strip(), params)


def get_db_connection():
    return psycopg2.connect(**get_db_config())


def next_day(report_date: str) -> str:
    dt = datetime.strptime(report_date, "%Y-%m-%d").date()
    return (dt + timedelta(days=1)).strftime("%Y-%m-%d")


def resolve_date_window(date_to: str, days_back: int) -> list[str]:
    end_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
    start_dt = end_dt - timedelta(days=days_back - 1)
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def apply_dry_run_limit(query: str, dry_run: bool) -> str:
    if dry_run:
        return f"{query.rstrip()}\nLIMIT {DRY_RUN_LIMIT}"
    return query


def validate_row(row: dict, required_fields: list[str]) -> tuple[bool, str | None]:
    for field in required_fields:
        value = row.get(field)
        if value is None:
            return False, f"missing_{field}"
        if isinstance(value, str) and not value.strip():
            return False, f"empty_{field}"
    return True, None


def extract_product_group(product_name):
    if not product_name:
        return "Без группы"

    name = str(product_name).strip()
    patterns = [
        "Аксессуары", "Брюки", "Дети", "Джемпер", "Куртки",
        "Обувь", "Платье", "Рубашки", "Сопутка", "Спорт",
        "Текстиль", "Трикотаж", "АКЦИЯ", "Наволочка", "Пододеяльник",
        "Простыня", "Полотенце",
    ]
    for pattern in patterns:
        if pattern.lower() in name.lower():
            return name.split(".")[0].strip() if "." in name else pattern
    if len(name) > 30:
        return name.split()[0] if " " in name else name[:30]
    return name


def get_unit_type(unit):
    if not unit:
        return "pcs"
    unit_str = str(unit).lower().strip()
    return "kg" if ("кг" in unit_str or "kg" in unit_str) else "pcs"


def extract_sales(cursor, report_date: str, *, dry_run: bool) -> list[tuple]:
    date_to = next_day(report_date)
    query = f"""
    SELECT
        s._Period AS sale_date_1c,
        w._Description AS warehouse,
        m._Description AS store,
        n._Description AS product,
        u._Description AS unit,
        s.{QUANTITY_COL} AS quantity,
        s.{REVENUE_COL} AS revenue,
        encode(s.{RECORDER_REF}, 'hex') AS recorder_id_hex,
        s._LineNo AS line_number
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
    LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
    LEFT JOIN _Reference387 n ON s.{NOMENCLATURE_REF} = n._IDRRef
    LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
    WHERE s._Period >= %s
      AND s._Period < %s
      AND s._RecorderTRef <> decode(%s, 'hex')
    ORDER BY s._Period
    """
    query = apply_dry_run_limit(query, dry_run)
    params = (f"{report_date} 00:00:00", f"{date_to} 00:00:00", EXCLUDED_RECORDER_TREF_HEX)
    log_sql("sales.extract", query, params)
    cursor.execute(query, params)
    return cursor.fetchall()


def transform_sales(rows: list[tuple]) -> tuple[list[dict], dict]:
    records = []
    skipped_rows = 0
    skip_reasons = defaultdict(int)

    for row in rows:
        sale_date, warehouse, store, product, unit, quantity, revenue, recorder_id_hex, line_number = row
        transformed = {
            "sale_date": sale_date.isoformat() if sale_date else None,
            "day_of_month": sale_date.day if sale_date else None,
            "week_number": sale_date.isocalendar()[1] if sale_date else None,
            "month": sale_date.month if sale_date else None,
            "quarter": ((sale_date.month - 1) // 3 + 1) if sale_date else None,
            "year": sale_date.year if sale_date else None,
            "weekday": sale_date.strftime("%A") if sale_date else None,
            "warehouse": warehouse,
            "store": store or warehouse,
            "product": product,
            "product_group": extract_product_group(product),
            "unit": unit,
            "unit_type": get_unit_type(unit),
            "quantity": float(quantity) if quantity else 0,
            "quantity_pcs": float(quantity) if quantity else 0,
            "quantity_kg": float(quantity) if quantity and get_unit_type(unit) == "kg" else 0,
            "revenue": float(revenue) if revenue else 0,
            "recorder_id": f"{recorder_id_hex}_{line_number}" if recorder_id_hex is not None else None,
        }
        valid, reason = validate_row(transformed, ["sale_date", "store", "product", "recorder_id"])
        if not valid:
            skipped_rows += 1
            skip_reasons[reason] += 1
            continue
        records.append(transformed)

    return records, {
        "skipped_rows": skipped_rows,
        "skip_reasons": dict(skip_reasons),
        "transform_errors": skipped_rows,
    }


def calculate_weight_and_category(product_group, product_name, qty_base, weights):
    category = "second"
    avg_weight = 0.0

    match = next(
        (
            w for w in weights
            if w.get("product_group") == product_group
            and w.get("product_name_pattern")
            and w["product_name_pattern"] in product_name
        ),
        None,
    )
    if not match:
        match = next(
            (w for w in weights if w.get("product_group") == product_group and not w.get("product_name_pattern")),
            None,
        )
    if not match:
        match = next(
            (
                w for w in weights
                if w.get("product_name_pattern")
                and w.get("product_group") in ("%", "АКЦИЯ")
                and w["product_name_pattern"] in product_name
            ),
            None,
        )

    if match:
        category = match.get("category", "second")
        avg_weight = float(match.get("avg_weight_kg") or 0.0)

    pn = str(product_name).lower()
    pg = str(product_group).lower()
    kp_kw = ["кпб", "пододеяльник", "простын", "наволоч", "комплект постельного", "полотен"]
    if any(k in pg or k in pn for k in kp_kw):
        category = "new"
        avg_weight = 0.0

    if category == "new":
        return 0.0, "new", "шт"

    calculated = float(qty_base) * avg_weight if (match and qty_base) else 0.0
    return calculated, category, "кг"


def extract_inventory(cursor, report_date: str, *, dry_run: bool) -> list[tuple]:
    report_dt = f"{report_date}T23:59:59.999999"
    query = """
    WITH stock AS (
        SELECT
            CAST(COALESCE(m._Description, w._Description) AS text) as store,
            CAST(n._Description AS text) as product,
            SUM(CASE WHEN s._RecordKind = 0 THEN s._Fld52575 ELSE -s._Fld52575 END) as quantity_base
        FROM _accumrg52568 s
        JOIN _Reference640 w ON s._Fld52573RRef = w._IDRRef
        LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
        JOIN _Reference387 n ON s._Fld52570RRef = n._IDRRef
        WHERE s._Active = true
          AND s._Period <= %s::timestamp
        GROUP BY
            CAST(COALESCE(m._Description, w._Description) AS text),
            CAST(n._Description AS text)
        HAVING SUM(CASE WHEN s._RecordKind = 0 THEN s._Fld52575 ELSE -s._Fld52575 END) <> 0
    )
    SELECT store, product, quantity_base FROM stock
    ORDER BY store, product
    """
    query = apply_dry_run_limit(query, dry_run)
    params = (report_dt,)
    log_sql("inventory.extract", query, params)
    cursor.execute(query, params)
    return cursor.fetchall()


def transform_inventory(rows: list[tuple], report_date: str, weights: list[dict]) -> tuple[list[dict], dict]:
    records = []
    skipped_rows = 0
    skip_reasons = defaultdict(int)

    for store, product, quantity_base in rows:
        store = store.strip() if store else None
        product = product.strip() if product else None
        product_group = extract_product_group(product)
        quantity_value = float(quantity_base) if quantity_base else 0.0
        quantity_calc, _, unit = calculate_weight_and_category(product_group, product, quantity_value, weights)
        quantity = quantity_calc if unit == "кг" else quantity_value

        transformed = {
            "store": store,
            "product": product,
            "quantity": round(quantity, 2),
            "product_group": product_group,
            "snapshot_date": report_date,
            "unit": unit,
        }
        valid, reason = validate_row(transformed, ["snapshot_date", "store", "product", "unit", "quantity"])
        if not valid:
            skipped_rows += 1
            skip_reasons[reason] += 1
            continue
        records.append(transformed)

    grouped = {}
    duplicate_groups = defaultdict(list)
    product_group_frequency = defaultdict(lambda: defaultdict(int))

    for record in records:
        key = (record["store"], record["product"], record["snapshot_date"], record["unit"])
        duplicate_groups[key].append(record)
        pg = record.get("product_group")
        if pg:
            product_group_frequency[key][pg] += 1

    deduped = []
    sample_duplicate_groups = []
    duplicate_group_count = 0

    for key, group_records in duplicate_groups.items():
        store, product, snapshot_date, unit = key
        if len(group_records) > 1:
            duplicate_group_count += 1
            if len(sample_duplicate_groups) < 5:
                sample_duplicate_groups.append(
                    {
                        "key": {
                            "store": store,
                            "product": product,
                            "snapshot_date": snapshot_date,
                            "unit": unit,
                        },
                        "rows_in_group": len(group_records),
                        "quantities": [row["quantity"] for row in group_records[:5]],
                    }
                )

        most_common_pg = None
        if product_group_frequency[key]:
            most_common_pg = sorted(
                product_group_frequency[key].items(),
                key=lambda item: (-item[1], item[0]),
            )[0][0]
        if not most_common_pg:
            most_common_pg = next((row.get("product_group") for row in group_records if row.get("product_group")), None)

        deduped.append(
            {
                "store": store,
                "product": product,
                "snapshot_date": snapshot_date,
                "unit": unit,
                "quantity": round(sum(float(row.get("quantity") or 0) for row in group_records), 2),
                "product_group": most_common_pg,
            }
        )

    return deduped, {
        "skipped_rows": skipped_rows,
        "skip_reasons": dict(skip_reasons),
        "transform_errors": skipped_rows,
        "rows_before_dedupe": len(records),
        "duplicate_groups_count": duplicate_group_count,
        "rows_after_dedupe": len(deduped),
        "sample_duplicate_groups": sample_duplicate_groups,
    }


def extract_visitors(cursor, report_date: str, *, dry_run: bool) -> list[tuple]:
    date_to = next_day(report_date)
    query = f"""
    SELECT
        v._Period::date AS visit_date,
        COALESCE(parent_store._Description, wh._Description, dev._Description) AS store,
        SUM(v.{VISITOR_COUNT_COL}) AS visitor_count
    FROM _AccumRg53554 v
    INNER JOIN _Reference648 dev ON v.{COUNTER_DEVICE_REF} = dev._IDRRef
    LEFT JOIN _Reference640 wh ON dev._Fld15930RRef = wh._IDRRef
    LEFT JOIN _Reference640 parent_store ON wh._ParentIDRRef = parent_store._IDRRef
    WHERE v._Period >= %s
      AND v._Period < %s
      AND v._Active = true
    GROUP BY v._Period::date, COALESCE(parent_store._Description, wh._Description, dev._Description)
    ORDER BY visit_date, store
    """
    query = apply_dry_run_limit(query, dry_run)
    params = (f"{report_date} 00:00:00", f"{date_to} 00:00:00")
    log_sql("visitors.extract", query, params)
    cursor.execute(query, params)
    return cursor.fetchall()


def transform_visitors(rows: list[tuple]) -> tuple[list[dict], dict]:
    records = []
    skipped_rows = 0
    skip_reasons = defaultdict(int)

    for visit_date, store, visitor_count in rows:
        transformed = {
            "visit_date": visit_date.isoformat() if visit_date else None,
            "store": store.strip() if store else None,
            "visitor_count": float(visitor_count) if visitor_count else 0,
        }
        valid, reason = validate_row(transformed, ["visit_date", "store", "visitor_count"])
        if not valid or transformed["visitor_count"] <= 0:
            skipped_rows += 1
            skip_reasons[reason or "non_positive_visitor_count"] += 1
            continue
        records.append(transformed)

    return records, {
        "skipped_rows": skipped_rows,
        "skip_reasons": dict(skip_reasons),
        "transform_errors": skipped_rows,
    }


def build_result(target: str, report_date: str, source_rows: int, rows: list[dict], meta: dict, count_before: int, insert_summary: dict) -> dict:
    return {
        "target": target,
        "table_name": get_target_table(target),
        "report_date": report_date,
        "dry_run": insert_summary["dry_run"],
        "source_rows_read": source_rows,
        "rows_after_transform": len(rows),
        "rows_to_insert": len(rows),
        "count_before": count_before,
        "count_after": None,
        "blocked": insert_summary.get("blocked", False),
        "blocked_reason": insert_summary.get("blocked_reason"),
        "inserted": insert_summary.get("inserted", 0),
        "insert_errors": insert_summary.get("errors", 0),
        "error_details": insert_summary.get("error_details", []),
        "skipped_rows": meta["skipped_rows"],
        "skip_reasons": meta["skip_reasons"],
        "transform_errors": meta["transform_errors"],
        "rows_before_dedupe": meta.get("rows_before_dedupe"),
        "duplicate_groups_count": meta.get("duplicate_groups_count"),
        "rows_after_dedupe": meta.get("rows_after_dedupe"),
        "sample_duplicate_groups": meta.get("sample_duplicate_groups", []),
        "sample_rows": sample_rows(rows),
    }


def summarize_sales_target(report_date: str) -> dict:
    table_name = get_target_table("sales")
    target_rows = fetch_rows_for_day(
        table_name,
        DATE_FIELDS["sales"],
        report_date,
        select_fields=f"recorder_id,revenue,{DATE_FIELDS['sales']}",
    )
    return {
        "target_rows": len(target_rows),
        "target_revenue": round(sum(float(row.get("revenue") or 0) for row in target_rows), 2),
    }


def summarize_visitors_target(report_date: str) -> dict:
    table_name = get_target_table("visitors")
    target_rows = fetch_rows_for_day(
        table_name,
        DATE_FIELDS["visitors"],
        report_date,
        select_fields=f"visit_date,store,visitor_count",
        exact_date=True,
    )
    return {
        "target_rows": len(target_rows),
        "target_visitor_count": round(sum(float(row.get("visitor_count") or 0) for row in target_rows), 2),
    }


def summarize_inventory_target(report_date: str) -> dict:
    table_name = get_target_table("inventory")
    target_rows = fetch_rows_for_day(
        table_name,
        DATE_FIELDS["inventory"],
        report_date,
        select_fields="snapshot_date,store,product,quantity",
        exact_date=True,
    )
    return {
        "target_rows": len(target_rows),
    }


def run_sales(cursor, report_date: str, *, dry_run: bool) -> dict:
    rows = extract_sales(cursor, report_date, dry_run=dry_run)
    transformed, meta = transform_sales(rows)
    table_name = get_target_table("sales")
    date_field = DATE_FIELDS["sales"]
    count_before = count_rows_for_date(table_name, date_field, report_date)
    source_revenue = round(sum(float(row["revenue"] or 0) for row in transformed), 2)
    insert_summary = upsert_rows(table_name, transformed, on_conflict="recorder_id", dry_run=dry_run)
    result = build_result("sales", report_date, len(rows), transformed, meta, count_before, insert_summary)
    target_summary = summarize_sales_target(report_date)
    result.update(
        {
            "source_revenue": source_revenue,
            "target_rows": target_summary["target_rows"],
            "target_revenue": target_summary["target_revenue"],
            "revenue_difference": round(source_revenue - target_summary["target_revenue"], 2),
        }
    )
    return result


def run_inventory(cursor, report_date: str, *, dry_run: bool) -> dict:
    weights = fetch_product_weights()
    rows = extract_inventory(cursor, report_date, dry_run=dry_run)
    transformed, meta = transform_inventory(rows, report_date, weights)
    table_name = get_target_table("inventory")
    date_field = DATE_FIELDS["inventory"]
    count_before = count_rows_for_date(table_name, date_field, report_date)
    delete_rows_for_date(table_name, date_field, report_date, dry_run=dry_run)
    insert_summary = insert_rows(table_name, transformed, dry_run=dry_run)
    result = build_result("inventory", report_date, len(rows), transformed, meta, count_before, insert_summary)
    target_summary = summarize_inventory_target(report_date)
    result.update(
        {
            "count_after": target_summary["target_rows"],
            "target_rows": target_summary["target_rows"],
            "delete_scope": f"{date_field} = {report_date}",
        }
    )
    return result


def run_visitors(cursor, report_date: str, *, dry_run: bool) -> dict:
    rows = extract_visitors(cursor, report_date, dry_run=dry_run)
    transformed, meta = transform_visitors(rows)
    table_name = get_target_table("visitors")
    date_field = DATE_FIELDS["visitors"]
    count_before = count_rows_for_date(table_name, date_field, report_date)
    source_visitor_count = round(sum(float(row["visitor_count"] or 0) for row in transformed), 2)
    insert_summary = upsert_rows(table_name, transformed, on_conflict="visit_date,store", dry_run=dry_run)
    result = build_result("visitors", report_date, len(rows), transformed, meta, count_before, insert_summary)
    target_summary = summarize_visitors_target(report_date)
    result.update(
        {
            "source_visitor_count": source_visitor_count,
            "target_rows": target_summary["target_rows"],
            "target_visitor_count": target_summary["target_visitor_count"],
            "visitor_count_difference": round(source_visitor_count - target_summary["target_visitor_count"], 2),
        }
    )
    return result


def print_result(result: dict) -> None:
    print()
    print("=" * 70)
    print(f"Target           : {result['target']}")
    print(f"Target table     : {result['table_name']}")
    print(f"Report date      : {result['report_date']}")
    print(f"Dry run          : {result['dry_run']}")
    print(f"Source rows read : {result['source_rows_read']}")
    print(f"Rows transformed : {result['rows_after_transform']}")
    if result["rows_before_dedupe"] is not None:
        print(f"Before dedupe    : {result['rows_before_dedupe']}")
    if result["duplicate_groups_count"] is not None:
        print(f"Duplicate groups : {result['duplicate_groups_count']}")
    if result["rows_after_dedupe"] is not None:
        print(f"After dedupe     : {result['rows_after_dedupe']}")
    print(f"Rows to insert   : {result['rows_to_insert']}")
    print(f"Count before     : {result['count_before']}")
    if result["count_after"] is not None:
        print(f"Count after      : {result['count_after']}")
    print(f"Inserted         : {result['inserted']}")
    print(f"Insert errors    : {result['insert_errors']}")
    if "target_rows" in result:
        print(f"Target rows      : {result['target_rows']}")
    if "source_revenue" in result:
        print(f"Source revenue   : {result['source_revenue']}")
    if "target_revenue" in result:
        print(f"Target revenue   : {result['target_revenue']}")
    if "revenue_difference" in result:
        print(f"Revenue diff     : {result['revenue_difference']}")
    if "source_visitor_count" in result:
        print(f"Source visitors  : {result['source_visitor_count']}")
    if "target_visitor_count" in result:
        print(f"Target visitors  : {result['target_visitor_count']}")
    if "visitor_count_difference" in result:
        print(f"Visitors diff    : {result['visitor_count_difference']}")
    print(f"Skipped rows     : {result['skipped_rows']}")
    print(f"Transform errors : {result['transform_errors']}")
    print(f"Blocked          : {result['blocked']}")
    if result["blocked_reason"]:
        print(f"Blocked reason   : {result['blocked_reason']}")
    if "delete_scope" in result:
        print(f"Delete scope     : {result['delete_scope']}")
    if result["error_details"]:
        print("Error details    :")
        for detail in result["error_details"]:
            print(detail)
    if result["sample_duplicate_groups"]:
        print("Duplicate samples:")
        for group in result["sample_duplicate_groups"]:
            print(group)
    print("Skip reasons     :", result["skip_reasons"])
    print("Sample rows      :")
    for row in result["sample_rows"]:
        print(row)
    print("=" * 70)


def main() -> int:
    args = parse_args()
    if args.date and args.date_to:
        raise SystemExit("Use either --date or --date-to, not both.")
    if not args.date and not args.date_to:
        raise SystemExit("One of --date or --date-to is required.")
    if args.days_back < 1:
        raise SystemExit("--days-back must be >= 1")

    report_dates = resolve_date_window(args.date_to, args.days_back) if args.date_to else [args.date]
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for report_date in report_dates:
            if args.target == "sales":
                result = run_sales(cursor, report_date, dry_run=args.dry_run)
            elif args.target == "inventory":
                result = run_inventory(cursor, report_date, dry_run=args.dry_run)
            else:
                result = run_visitors(cursor, report_date, dry_run=args.dry_run)
            print_result(result)
        return 0
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
