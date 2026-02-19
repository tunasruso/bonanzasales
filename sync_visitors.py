#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
Sync Visitors (Traffic Counters) from 1C:Retail → Supabase
═══════════════════════════════════════════════════════════════════════════════

Source: Register _AccumRg53554 (Посетители)
  - _Period → visit_date
  - _Fld53555RRef → Warehouse reference → Store name
  - _Fld53556 → visitor_count (количество посетителей)

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

# Warehouse reference column in _AccumRg53554
WAREHOUSE_REF = '_Fld53555RRef'
VISITOR_COUNT_COL = '_Fld53556'

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
    """Extract visitor data from 1C register _AccumRg53554."""
    log.info(f"Extracting visitors data since {start_date}...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Aggregate by date + store (parent warehouse)
    query = f"""
    SELECT 
        v._Period::date AS visit_date,
        COALESCE(m._Description, w._Description) AS store,
        SUM(v.{VISITOR_COUNT_COL}) AS visitor_count
    FROM _AccumRg53554 v
    INNER JOIN _Reference640 w ON v.{WAREHOUSE_REF} = w._IDRRef
    LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
    WHERE v._Period >= %s
      AND v._Active = true
    GROUP BY v._Period::date, COALESCE(m._Description, w._Description)
    ORDER BY visit_date, store
    """

    cursor.execute(query, (start_date,))
    rows = cursor.fetchall()
    log.info(f"Fetched {len(rows):,} aggregated visitor records")

    records = []
    for row in rows:
        visit_date, store, visitor_count = row
        if store and visitor_count:
            records.append({
                'visit_date': visit_date.isoformat(),
                'store': store.strip(),
                'visitor_count': float(visitor_count)
            })

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
    print("  LiderTeks Visitors (Traffic) Sync: 1C → Supabase")
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
