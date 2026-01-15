#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
LiderTeks 1C → Supabase Sales Sync
═══════════════════════════════════════════════════════════════════════════════
Extracts ALL sales data from 1C MS SQL and uploads to Supabase for analytics

Author: Antigravity
Date: 2026-01-15
═══════════════════════════════════════════════════════════════════════════════
"""

import pymssql
import requests
import json
import logging
from datetime import datetime
from decimal import Decimal

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# 1C MS SQL Database
DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

# Supabase
SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

# Column mappings (verified)
WAREHOUSE_REF = '_Fld53725RRef'
NOMENCLATURE_REF = '_Fld53716RRef'
REVENUE_COL = '_Fld53732'
QUANTITY_COL = '_Fld53731'
RECORDER_REF = '_RecorderRRef'

# Date offset
DATE_OFFSET_YEARS = 2000

# Batch size for Supabase inserts
BATCH_SIZE = 500

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

def get_1c_connection():
    return pymssql.connect(**DB_CONFIG)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA EXTRACTION FROM 1C
# ═══════════════════════════════════════════════════════════════════════════════

def extract_all_sales(cursor):
    """Extract ALL sales data from 1C database."""
    log.info("Extracting ALL sales data from 1C...")
    
    query = f"""
    SELECT 
        s._Period AS sale_date_1c,
        w._Description AS warehouse,
        m._Description AS store,
        n._Description AS product,
        u._Description AS unit,
        s.{QUANTITY_COL} AS quantity,
        s.{REVENUE_COL} AS revenue,
        CONVERT(VARCHAR(50), s.{RECORDER_REF}, 2) AS recorder_id
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
    LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
    LEFT JOIN _Reference387 n ON s.{NOMENCLATURE_REF} = n._IDRRef
    LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
    ORDER BY s._Period
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    log.info(f"Fetched {len(rows):,} total sales records")
    
    return rows


def convert_1c_date(dt_1c):
    """Convert 1C date (with +2000 year offset) to real date."""
    if dt_1c is None:
        return None
    try:
        dt_str = str(dt_1c)
        if len(dt_str) >= 4:
            year_1c = int(dt_str[:4])
            year_real = year_1c - DATE_OFFSET_YEARS
            new_dt_str = str(year_real) + dt_str[4:]
            return datetime.strptime(new_dt_str[:10], '%Y-%m-%d').date()
    except:
        pass
    return None


def extract_product_group(product_name):
    """Extract product group from product name."""
    if not product_name:
        return 'Без группы'
    
    name = str(product_name).strip()
    
    patterns = [
        'Аксессуары', 'Брюки', 'Дети', 'Джемпер', 'Куртки', 
        'Обувь', 'Платье', 'Рубашки', 'Сопутка', 'Спорт',
        'Текстиль', 'Трикотаж', 'АКЦИЯ', 'Наволочка', 'Пододеяльник',
        'Простыня', 'Полотенце'
    ]
    
    for pattern in patterns:
        if pattern.lower() in name.lower():
            if '.' in name:
                return name.split('.')[0].strip()
            return pattern
    
    if len(name) > 30:
        return name.split()[0] if ' ' in name else name[:30]
    return name


def get_unit_type(unit):
    """Determine unit type (kg or pcs)."""
    if not unit:
        return 'pcs'
    unit_str = str(unit).lower().strip()
    if 'кг' in unit_str or 'kg' in unit_str:
        return 'kg'
    return 'pcs'


def transform_row(row):
    """Transform a raw database row into Supabase record format."""
    sale_date_1c, warehouse, store, product, unit, quantity, revenue, recorder_id = row
    
    sale_date = convert_1c_date(sale_date_1c)
    if not sale_date:
        return None
    
    # Handle None store
    store = store if store else warehouse
    if not store:
        return None
    
    product_group = extract_product_group(product)
    unit_type = get_unit_type(unit)
    
    quantity = float(quantity) if quantity else 0
    revenue = float(revenue) if revenue else 0
    
    return {
        'sale_date': sale_date.isoformat(),
        'day_of_month': sale_date.day,
        'week_number': sale_date.isocalendar()[1],
        'month': sale_date.month,
        'quarter': (sale_date.month - 1) // 3 + 1,
        'year': sale_date.year,
        'weekday': sale_date.strftime('%A'),
        'warehouse': warehouse,
        'store': store,
        'product': product,
        'product_group': product_group,
        'unit': unit,
        'unit_type': unit_type,
        'quantity': quantity,
        'quantity_pcs': quantity,
        'quantity_kg': quantity if unit_type == 'kg' else 0,
        'revenue': revenue,
        'recorder_id': recorder_id
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SUPABASE UPLOAD
# ═══════════════════════════════════════════════════════════════════════════════

def clear_supabase_table():
    """Clear existing data from Supabase table."""
    log.info("Clearing existing data from Supabase...")
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    
    # Delete all rows
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics?id=gt.0"
    response = requests.delete(url, headers=headers)
    
    if response.status_code in [200, 204]:
        log.info("✅ Cleared Supabase table")
    else:
        log.warning(f"Clear response: {response.status_code} - {response.text}")


def upload_to_supabase(records):
    """Upload records to Supabase in batches."""
    log.info(f"Uploading {len(records):,} records to Supabase...")
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics"
    
    total_uploaded = 0
    errors = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        try:
            response = requests.post(url, headers=headers, json=batch)
            
            if response.status_code in [200, 201]:
                total_uploaded += len(batch)
                if (i // BATCH_SIZE + 1) % 10 == 0:
                    log.info(f"  Uploaded {total_uploaded:,} / {len(records):,} records...")
            else:
                errors += 1
                log.error(f"  Batch error: {response.status_code} - {response.text[:200]}")
                
        except Exception as e:
            errors += 1
            log.error(f"  Upload exception: {e}")
    
    log.info(f"✅ Upload complete: {total_uploaded:,} records, {errors} errors")
    return total_uploaded


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print()
    print("═" * 70)
    print("  LiderTeks 1C → Supabase Sales Sync")
    print("═" * 70)
    print()
    
    # Connect to 1C
    log.info("Connecting to 1C MS SQL...")
    conn = get_1c_connection()
    cursor = conn.cursor()
    log.info("✅ Connected to 1C")
    
    # Extract all sales
    rows = extract_all_sales(cursor)
    
    # Transform data
    log.info("Transforming data...")
    records = []
    skipped = 0
    
    for row in rows:
        record = transform_row(row)
        if record:
            records.append(record)
        else:
            skipped += 1
    
    log.info(f"Transformed {len(records):,} records ({skipped} skipped)")
    
    # Close 1C connection
    cursor.close()
    conn.close()
    log.info("🔌 Disconnected from 1C")
    
    # Clear and upload to Supabase
    clear_supabase_table()
    uploaded = upload_to_supabase(records)
    
    # Summary
    print()
    print("═" * 70)
    log.info("SYNC COMPLETE")
    print("═" * 70)
    
    # Get date range
    dates = [r['sale_date'] for r in records]
    min_date = min(dates) if dates else 'N/A'
    max_date = max(dates) if dates else 'N/A'
    
    # Get unique counts
    stores = set(r['store'] for r in records)
    groups = set(r['product_group'] for r in records)
    
    total_revenue = sum(r['revenue'] for r in records)
    total_kg = sum(r['quantity_kg'] for r in records)
    total_pcs = sum(r['quantity_pcs'] for r in records)
    
    log.info(f"Период данных:     {min_date} — {max_date}")
    log.info(f"Всего записей:     {len(records):,}")
    log.info(f"Магазинов:         {len(stores)}")
    log.info(f"Товарных групп:    {len(groups)}")
    log.info(f"Общая выручка:     {total_revenue:,.2f} ₽")
    log.info(f"Всего кг:          {total_kg:,.2f}")
    log.info(f"Всего шт:          {total_pcs:,.0f}")
    log.info(f"Supabase URL:      {SUPABASE_URL}")
    
    return 0


if __name__ == "__main__":
    exit(main())
