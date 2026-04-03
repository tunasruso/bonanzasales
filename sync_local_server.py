#!/usr/bin/env python3
"""
LiderTeks 1C -> Supabase Sales Sync
Запускается локально на 172.16.0.45
Подключается к 1C PostgreSQL через туннель: localhost:5444

Использование:
    python3 sync_local_server.py                  # с DEFAULT_START_DATE
    python3 sync_local_server.py 2026-01-01       # с указанной даты
    python3 sync_local_server.py 2026-03-01       # только март
"""

import sys
import requests
import json
import logging
from datetime import datetime
from decimal import Decimal
import psycopg2

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# 1C PostgreSQL — туннель пробрасывает порт 5444 на localhost
DB_CONFIG = {
    'host':     'localhost',
    'port':     5444,
    'user':     'ecostock',
    'password': 'Kd*2m5Th',
    'dbname':   'onec_ecostock_retail',
}

# Supabase
SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

# С какой даты синхронизировать по умолчанию
DEFAULT_START_DATE = '2025-09-01'

# Размер батча для Supabase
BATCH_SIZE = 500

# Колонки регистра накопления 1C (не менять)
WAREHOUSE_REF    = '_Fld53725RRef'
NOMENCLATURE_REF = '_Fld53716RRef'
REVENUE_COL      = '_Fld53732'
QUANTITY_COL     = '_Fld53731'
RECORDER_REF     = '_RecorderRRef'
# Document1009 = Расходная накладная. Это перемещение на другое юрлицо,
# а не розничная продажа, поэтому не должно попадать в sales_analytics.
EXCLUDED_RECORDER_TREF_HEX = '000003f1'

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)
log = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


# ═══════════════════════════════════════════════════════════════════════════════
# ИЗВЛЕЧЕНИЕ ИЗ 1C
# ═══════════════════════════════════════════════════════════════════════════════

def get_db_connection():
    log.info(f"Connecting: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']}")
    conn = psycopg2.connect(**DB_CONFIG)
    log.info("✅ Connected to 1C PostgreSQL")
    return conn


def extract_sales(cursor, start_date):
    log.info(f"Extracting sales since {start_date}...")
    query = f"""
    SELECT
        s._Period                        AS sale_date,
        w._Description                   AS warehouse,
        m._Description                   AS store,
        n._Description                   AS product,
        u._Description                   AS unit,
        s.{QUANTITY_COL}                 AS quantity,
        s.{REVENUE_COL}                  AS revenue,
        encode(s.{RECORDER_REF}, 'hex') AS recorder_id_hex,
        s._LineNo                        AS line_number
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF}    = w._IDRRef
    LEFT  JOIN _Reference640 m ON w._ParentIDRRef       = m._IDRRef
    LEFT  JOIN _Reference387 n ON s.{NOMENCLATURE_REF} = n._IDRRef
    LEFT  JOIN _Reference188 u ON n._Fld9817RRef        = u._IDRRef
    WHERE s._Period >= %s
      AND s._RecorderTRef <> decode(%s, 'hex')
    ORDER BY s._Period
    """
    cursor.execute(query, (f"{start_date} 00:00:00", EXCLUDED_RECORDER_TREF_HEX))
    rows = cursor.fetchall()
    log.info(f"Fetched {len(rows):,} rows from 1C")
    return rows


def extract_product_group(name):
    if not name:
        return 'Без группы'
    name = str(name).strip()
    patterns = [
        'Аксессуары', 'Брюки', 'Дети', 'Джемпер', 'Куртки',
        'Обувь', 'Платье', 'Рубашки', 'Сопутка', 'Спорт',
        'Текстиль', 'Трикотаж', 'АКЦИЯ', 'Наволочка', 'Пододеяльник',
        'Простыня', 'Полотенце',
    ]
    for p in patterns:
        if p.lower() in name.lower():
            return name.split('.')[0].strip() if '.' in name else p
    return name.split()[0] if len(name) > 30 and ' ' in name else name[:30] if len(name) > 30 else name


def get_unit_type(unit):
    if not unit:
        return 'pcs'
    s = str(unit).lower()
    return 'kg' if ('кг' in s or 'kg' in s) else 'pcs'


def transform_row(row):
    sale_date, warehouse, store, product, unit, quantity, revenue, recorder_hex, line_no = row
    if not sale_date:
        return None
    store = store or warehouse
    if not store:
        return None

    pg      = extract_product_group(product)
    ut      = get_unit_type(unit)
    qty     = float(quantity) if quantity else 0.0
    rev     = float(revenue)  if revenue  else 0.0

    # sale_date из PostgreSQL приходит как datetime, берём только дату
    sd = sale_date.date() if hasattr(sale_date, 'date') else sale_date

    return {
        'sale_date':     sd.isoformat(),
        'day_of_month':  sd.day,
        'week_number':   sd.isocalendar()[1],
        'month':         sd.month,
        'quarter':       (sd.month - 1) // 3 + 1,
        'year':          sd.year,
        'weekday':       sd.strftime('%A'),
        'warehouse':     warehouse,
        'store':         store,
        'product':       product,
        'product_group': pg,
        'unit':          unit,
        'unit_type':     ut,
        'quantity':      qty,
        'quantity_pcs':  qty,
        'quantity_kg':   qty if ut == 'kg' else 0.0,
        'revenue':       rev,
        'recorder_id':   f"{recorder_hex}_{line_no}",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА В SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════

def upload_to_supabase(records):
    log.info(f"Uploading {len(records):,} records to Supabase...")
    headers = {
        'apikey':        SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type':  'application/json',
        'Prefer':        'resolution=merge-duplicates',
    }
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics?on_conflict=recorder_id"

    uploaded = 0
    errors   = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            r = requests.post(url, headers=headers, data=json.dumps(batch, cls=DecimalEncoder), timeout=30)
            if r.status_code in (200, 201):
                uploaded += len(batch)
                if (i // BATCH_SIZE + 1) % 10 == 0:
                    log.info(f"  {uploaded:,} / {len(records):,} uploaded...")
            else:
                errors += 1
                log.error(f"  Batch {i//BATCH_SIZE+1} HTTP {r.status_code}: {r.text[:300]}")
        except Exception as e:
            errors += 1
            log.error(f"  Batch {i//BATCH_SIZE+1} exception: {e}")

    log.info(f"✅ Upload complete: {uploaded:,} ok, {errors} errors")
    return uploaded


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    start_date = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_START_DATE

    print()
    print("=" * 60)
    print("  LiderTeks 1C -> Supabase  (local server sync)")
    print(f"  Start date : {start_date}")
    print(f"  DB         : {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print("=" * 60)
    print()

    # 1. Подключение
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        return 1

    # 2. Извлечение
    try:
        rows = extract_sales(cursor, start_date)
    except Exception as e:
        log.error(f"Extraction failed: {e}")
        return 1
    finally:
        cursor.close()
        conn.close()

    # 3. Трансформация
    log.info("Transforming...")
    records = []
    skipped = 0
    for row in rows:
        rec = transform_row(row)
        if rec:
            records.append(rec)
        else:
            skipped += 1
    log.info(f"Transformed: {len(records):,} ok, {skipped} skipped")

    if not records:
        log.info("Nothing to upload.")
        return 0

    # 4. Дедупликация
    unique = {r['recorder_id']: r for r in records}
    deduped = list(unique.values())
    log.info(f"Deduped: {len(deduped):,} unique (removed {len(records)-len(deduped)} duplicates)")

    # 5. Загрузка
    upload_to_supabase(deduped)

    print()
    print("=" * 60)
    log.info("SYNC COMPLETE")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
