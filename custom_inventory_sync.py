#!/usr/bin/env python3
"""
Inventory Sync: Load stock balances from 1C register "ЗапасыНаСкладах" (AccumRg52568)
as of a specific date into Supabase inventory_analytics table.

Key insight: In 1C:Retail (Розница), quantities are stored in the base unit (шт = pieces).
For secondhand clothing, there's also a KG unit with a conversion coefficient.
We convert weighted products to KG using: quantity_kg = quantity_base / kg_coefficient

Usage:
  python custom_inventory_sync.py                  # stock as of today
  python custom_inventory_sync.py 2026-02-19       # stock as of specific date
"""

import psycopg2
import os
import sys
import requests
import json
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date

# Configuration
SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"
TABLE_NAME = "inventory_analytics"


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', '100.91.185.91'),
        user=os.getenv('POSTGRES_USER', 'ecostock'),
        password=os.getenv('POSTGRES_PASSWORD', 'Kd*2m5Th'),
        dbname=os.getenv('POSTGRES_DB', 'onec_ecostock_retail'),
        port=os.getenv('POSTGRES_PORT', 5444),
        connect_timeout=10
    )


def fetch_product_weights():
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    r = requests.get(f"{SUPABASE_URL}/rest/v1/product_weights?select=*", headers=headers)
    r.raise_for_status()
    return r.json()

def calculate_weight_and_category(product_group, product_name, qty_base, weights):
    category = 'second'
    avg_weight = 0.0
    
    match = next((w for w in weights if w.get('product_group') == product_group and w.get('product_name_pattern') and w['product_name_pattern'] in product_name), None)
    if not match:
        match = next((w for w in weights if w.get('product_group') == product_group and not w.get('product_name_pattern')), None)
    if not match:
        match = next((w for w in weights if w.get('product_name_pattern') and w.get('product_group') in ('%', 'АКЦИЯ') and w['product_name_pattern'] in product_name), None)
        
    if match:
        category = match.get('category', 'second')
        avg_weight = float(match.get('avg_weight_kg') or 0.0)
    
    # Fallback for KP missing in DB
    pn = str(product_name).lower()
    pg = str(product_group).lower()
    kp_kw = ['кпб', 'пододеяльник', 'простын', 'наволоч', 'комплект постельного', 'полотен']
    if any(k in pg or k in pn for k in kp_kw):
        category = 'new'
        avg_weight = 0.0
        
    if category == 'new':
        return 0.0, 'new', 'шт'
        
    calculated = float(qty_base) * avg_weight if (match and qty_base) else 0.0
    return calculated, category, 'кг'

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

def extract_inventory(report_date: str):
    """
    Extract stock balances from 1C register (ЗапасыНаСкладах) as of a specific date.
    
    Converts weighted products (секонд-хенд) from base units (шт) to KG 
    using the product_weights table from Supabase.
    """
    print(f"Fetching product weights from Supabase...")
    weights = fetch_product_weights()
    print(f"Loaded {len(weights)} weight rules.")
    
    print(f"Connecting to PostgreSQL (1C database)...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    next_day = f"{report_date}T23:59:59.999999"
    
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
    
    print(f"Calculating stock as of {report_date}...")
    cursor.execute(query, (next_day,))
    rows = cursor.fetchall()
    print(f"Extracted {len(rows)} inventory records.")
    
    # Build data, aggregating by (store, product, unit) to handle duplicates
    agg = defaultdict(lambda: {'quantity': 0.0, 'product_group': 'Unknown'})
    store_kg = defaultdict(float)
    store_pcs = defaultdict(float)
    
    for row in rows:
        store = row[0].strip() if row[0] else 'Unknown'
        product = row[1].strip() if row[1] else 'Unknown'
        quantity_base = float(row[2])
        product_group = extract_product_group(product)
        
        quantity_kg, category, unit = calculate_weight_and_category(product_group, product, quantity_base, weights)
        
        if unit == 'кг':
            qty = quantity_kg
            store_kg[store] += quantity_kg
        else:
            qty = quantity_base
            store_pcs[store] += quantity_base
        
        key = (store, product, unit)
        agg[key]['quantity'] += qty
        agg[key]['product_group'] = product_group
    
    inventory_data = []
    for (store, product, unit), v in agg.items():
        if v['quantity'] == 0:
            continue
        inventory_data.append({
            "store": store,
            "product": product,
            "quantity": round(v['quantity'], 2),
            "product_group": v['product_group'],
            "snapshot_date": report_date,
            "unit": unit
        })
    
    # Print summary
    all_stores = sorted(set(list(store_kg.keys()) + list(store_pcs.keys())))
    print(f"\n{'Store':30s} {'КГ':>12s} {'ШТ (other)':>12s}")
    print('-' * 58)
    total_kg = total_pcs = 0
    for s in all_stores:
        kg = store_kg.get(s, 0)
        pcs = store_pcs.get(s, 0)
        total_kg += kg
        total_pcs += pcs
        print(f"  {s:28s} {kg:>12,.1f} {pcs:>12,.0f}")
    print('-' * 58)
    print(f"  {'ИТОГО':28s} {total_kg:>12,.1f} {total_pcs:>12,.0f}")
    
    conn.close()
    return inventory_data


def upload_to_supabase(data, report_date: str):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    # Delete existing data for this snapshot_date
    print(f"\nClearing existing inventory for {report_date}...")
    delete_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?snapshot_date=eq.{report_date}"
    r = requests.delete(delete_url, headers=headers)
    print(f"Delete status: {r.status_code}")
    
    # Upload in batches
    batch_size = 500
    total_uploaded = 0
    errors = 0
    
    print(f"Uploading {len(data)} records to Supabase...")
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
        
        response = requests.post(
            url, 
            headers=headers, 
            data=json.dumps(batch, cls=DecimalEncoder)
        )
        
        if response.status_code not in (200, 201):
            print(f"Error uploading batch {i}: {response.text[:200]}")
            errors += 1
        else:
            total_uploaded += len(batch)
            print(f"Uploaded {total_uploaded}/{len(data)} records")
    
    print(f"\nDone: {total_uploaded} uploaded, {errors} errors")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        report_date = sys.argv[1]
    else:
        report_date = date.today().strftime('%Y-%m-%d')
    
    print(f"=== Inventory Sync for {report_date} ===\n")
    
    try:
        data = extract_inventory(report_date)
        if data:
            upload_to_supabase(data, report_date)
            print(f"\n✅ Sync completed for {report_date}")
        else:
            print("No data found.")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
