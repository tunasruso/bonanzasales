#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Sync Visitors (Traffic Counters) from 1C:Retail → Supabase
═══════════════════════════════════════════════════════════════════════════════

Source: Register _AccumRg53554 (Посетители)
  Dimension: _Fld53555RRef → _Reference648 (Оборудование подсчёта посетителей)
    → _Reference648._Fld15930RRef → _Reference640 (Склад) → parent store name
  Resources:
    _Fld53556 = КоличествоЧеков (check count) 
    _Fld53557 = КоличествоПокупателей (buyer count)
    _Fld53558 = КоличествоПосетителей (VISITOR COUNT)

Target: Supabase table `visitors_analytics`
  - visit_date, store, visitor_count

═══════════════════════════════════════════════════════════════════════════════
"""

import sys
import os
import json
import logging
import requests
import psycopg2
from datetime import datetime
from decimal import Decimal

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
    'user': os.getenv('POSTGRES_USER', 'ecostock'),
    'password': os.getenv('POSTGRES_PASSWORD', 'Kd*2m5Th'),
    'dbname': os.getenv('POSTGRES_DB', 'onec_ecostock_retail'),
    'port': os.getenv('POSTGRES_PORT', 5432)
}

SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

# Register fields
COUNTER_DEVICE_REF = '_Fld53555RRef'  # → _Reference648 (counter device)
CHECK_COUNT_COL    = '_Fld53556'      # Количество чеков
BUYER_COUNT_COL    = '_Fld53557'      # Количество покупателей
VISITOR_COUNT_COL  = '_Fld53558'      # Количество посетителей (main metric)

BATCH_SIZE = 500

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
# EXTRACT FROM 1C
# ═══════════════════════════════════════════════════════════════════════════════

def extract_visitors(start_date='2026-01-01'):
    """Extract visitor data from 1C register _AccumRg53554.
    
    Chain: _AccumRg53554._Fld53555RRef 
           → _Reference648 (counter device, e.g. "Озерки")
           → _Reference648._Fld15930RRef 
           → _Reference640 (warehouse, e.g. "Магазин (Озерки) Торговый зал")
           → _Reference640._ParentIDRRef
           → _Reference640 (parent store, e.g. "Озерки")
    """
    log.info(f"Extracting visitors data since {start_date}...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Join through: register → counter device → warehouse → parent store
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
      AND v._Active = true
    GROUP BY v._Period::date, COALESCE(parent_store._Description, wh._Description, dev._Description)
    ORDER BY visit_date, store
    """

    cursor.execute(query, (start_date,))
    rows = cursor.fetchall()
    log.info(f"Fetched {len(rows):,} aggregated visitor records")

    records = []
    for row in rows:
        visit_date, store, visitor_count = row
        store_name = store.strip() if store else None
        count = float(visitor_count) if visitor_count else 0
        
        if store_name and count > 0:
            records.append({
                'visit_date': visit_date.isoformat(),
                'store': store_name,
                'visitor_count': count
            })
            log.info(f"  {visit_date} | {store_name:20s} | {count:.0f} visitors")

    cursor.close()
    conn.close()

    return records


# ═══════════════════════════════════════════════════════════════════════════════
# UPLOAD TO SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════

def upload_visitors(records):
    """Upload visitor records to Supabase using UPSERT."""
    if not records:
        log.info("No visitor records to upload.")
        return 0

    log.info(f"Uploading {len(records):,} visitor records to Supabase...")

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }

    url = f"{SUPABASE_URL}/rest/v1/visitors_analytics?on_conflict=visit_date,store"

    total_uploaded = 0
    errors = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(batch, cls=DecimalEncoder)
            )
            if response.status_code in [200, 201]:
                total_uploaded += len(batch)
            else:
                errors += 1
                log.error(f"Batch error: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            errors += 1
            log.error(f"Upload exception: {e}")

    log.info(f"✅ Visitors upload: {total_uploaded:,} records, {errors} errors")
    return total_uploaded


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print()
    print("═" * 70)
    print("  Bonanza Visitors (Traffic) Sync: 1C → Supabase")
    print("═" * 70)

    records = extract_visitors()

    if records:
        upload_visitors(records)
    else:
        log.info("No visitor data found in 1C register (_AccumRg53554).")
        log.info("Counters may not be configured yet in 1C:Retail.")

    print("═" * 70)
    log.info("VISITOR SYNC COMPLETE")
    print("═" * 70)


if __name__ == "__main__":
    main()
