#!/usr/bin/env python3
"""
1C:Retail 3.0 - Full Sales Report with Nomenclature
====================================================
December 2025 - All stores with product breakdown
"""

import pymssql
from decimal import Decimal

DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

# Verified column mappings
WAREHOUSE_REF = '_Fld53725RRef'  # Reference to warehouse (Reference640)
REVENUE_COL = '_Fld53732'        # Revenue
QUANTITY_COL = '_Fld53731'       # Quantity


def get_connection():
    return pymssql.connect(**DB_CONFIG)


def find_nomenclature_ref_column(cursor):
    """Find which column references nomenclature."""
    print("=" * 70)
    print("Finding nomenclature reference column...")
    print("=" * 70)
    
    # Get all RRef columns
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '_AccumRg53715'
    AND COLUMN_NAME LIKE '%RRef'
    ORDER BY COLUMN_NAME
    """
    cursor.execute(query)
    ref_cols = [r[0] for r in cursor.fetchall()]
    
    # Get sample nomenclature IDs
    query = """
    SELECT TOP 5 _IDRRef FROM _Reference387
    """
    cursor.execute(query)
    nomen_ids = [r[0] for r in cursor.fetchall()]
    
    if not nomen_ids:
        print("No nomenclature found!")
        return None
    
    placeholders = ', '.join(['%s'] * len(nomen_ids))
    
    for col in ref_cols:
        if col in [WAREHOUSE_REF, '_RecorderRRef']:
            continue
            
        query = f"""
        SELECT COUNT(*) 
        FROM _AccumRg53715
        WHERE {col} IN ({placeholders})
        AND _Period >= '4025-12-01' AND _Period < '4026-01-01'
        """
        try:
            cursor.execute(query, nomen_ids)
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"✅ {col}: {count} records match nomenclature")
                return col
        except:
            pass
    
    return None


def test_nomenclature_join(cursor, nomen_col):
    """Test join with nomenclature and show sample data."""
    print(f"\n" + "=" * 70)
    print(f"Testing nomenclature join with column: {nomen_col}")
    print("=" * 70)
    
    query = f"""
    SELECT TOP 10
        n._Description AS Product,
        SUM(s.{QUANTITY_COL}) AS Qty,
        SUM(s.{REVENUE_COL}) AS Revenue
    FROM _AccumRg53715 s
    LEFT JOIN _Reference387 n ON s.{nomen_col} = n._IDRRef
    WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
    GROUP BY n._Description
    ORDER BY Revenue DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"\nTop 10 products by revenue:")
    for row in rows:
        prod = row[0][:50] if row[0] else "(NULL)"
        qty = row[1] or 0
        rev = row[2] or 0
        print(f"  {prod:<50} {float(qty):>10,.2f} {float(rev):>12,.2f}")


def generate_full_report(cursor, nomen_col):
    """Generate full report: Store → Nomenclature breakdown."""
    print("\n" + "=" * 70)
    print("FULL SALES REPORT - DECEMBER 2025")
    print("=" * 70)
    
    # Main query with warehouse and nomenclature joins
    query = f"""
    SELECT 
        w._Description AS Warehouse,
        n._Description AS Product,
        SUM(s.{QUANTITY_COL}) AS Quantity,
        SUM(s.{REVENUE_COL}) AS Revenue
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
    LEFT JOIN _Reference387 n ON s.{nomen_col} = n._IDRRef
    WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
    GROUP BY w._Description, n._Description
    ORDER BY w._Description, Revenue DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Process and display results
    current_store = None
    store_qty = Decimal(0)
    store_rev = Decimal(0)
    grand_qty = Decimal(0)
    grand_rev = Decimal(0)
    
    output_lines = []
    
    for row in rows:
        warehouse, product, qty, rev = row
        qty = qty or Decimal(0)
        rev = rev or Decimal(0)
        
        if warehouse != current_store:
            # Print previous store total if exists
            if current_store:
                output_lines.append(f"  {'─' * 60}")
                output_lines.append(f"  {'ИТОГО по магазину:':<45} {float(store_qty):>10,.3f} {float(store_rev):>12,.2f}")
                output_lines.append("")
            
            current_store = warehouse
            store_qty = Decimal(0)
            store_rev = Decimal(0)
            
            output_lines.append(f"{'═' * 80}")
            output_lines.append(f"📍 {warehouse}")
            output_lines.append(f"{'─' * 80}")
            output_lines.append(f"  {'Номенклатура':<45} {'Кол-во':>10} {'Выручка':>12}")
            output_lines.append(f"  {'─' * 70}")
        
        store_qty += qty
        store_rev += rev
        grand_qty += qty
        grand_rev += rev
        
        prod_name = product[:45] if product else "(Не указано)"
        output_lines.append(f"  {prod_name:<45} {float(qty):>10,.3f} {float(rev):>12,.2f}")
    
    # Final store total
    if current_store:
        output_lines.append(f"  {'─' * 60}")
        output_lines.append(f"  {'ИТОГО по магазину:':<45} {float(store_qty):>10,.3f} {float(store_rev):>12,.2f}")
    
    # Grand total
    output_lines.append("")
    output_lines.append("═" * 80)
    output_lines.append(f"{'ИТОГО ПО ВСЕМ МАГАЗИНАМ:':<50} {float(grand_qty):>10,.3f} {float(grand_rev):>12,.2f}")
    output_lines.append("═" * 80)
    
    # Print all lines
    for line in output_lines:
        print(line)
    
    return grand_qty, grand_rev


def verify_totals(cursor, nomen_col):
    """Verify totals match 1C report."""
    print("\n" + "=" * 70)
    print("VERIFICATION: Comparing with 1C data")
    print("=" * 70)
    
    # Reference values from 1C
    expected_stores = {
        'Большевиков': (2048.0, 776661.0),
        'Иваново': (983.0, 184400.0),
        'Измайлово': (1560.0, 466674.0),
        'Коломна': (1250.0, 427082.0),
        'Озерки': (4890.0, 1560876.0),
        'Орёл': (3319.0, 692508.0),
        'Просвещения': (4498.0, 1050563.0),
        'Тверь': (4132.0, 903676.0),
        'Туристская': (4112.0, 1657767.0),
    }
    
    expected_total = (26792.0, 7720207.0)
    
    # Query actual totals by store
    query = f"""
    SELECT 
        w._Description AS Warehouse,
        SUM(s.{QUANTITY_COL}) AS Quantity,
        SUM(s.{REVENUE_COL}) AS Revenue
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
    WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
    GROUP BY w._Description
    ORDER BY w._Description
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"\n{'Магазин':<45} {'Кол-во (факт)':>12} {'Кол-во (1C)':>12} {'✓':>3}")
    print("-" * 75)
    
    actual_total_qty = Decimal(0)
    actual_total_rev = Decimal(0)
    
    for row in rows:
        warehouse, qty, rev = row
        qty = qty or Decimal(0)
        rev = rev or Decimal(0)
        
        actual_total_qty += qty
        actual_total_rev += rev
        
        # Find matching expected
        for store_key, (exp_qty, exp_rev) in expected_stores.items():
            if store_key in warehouse:
                qty_match = abs(float(qty) - exp_qty) < 1
                rev_match = abs(float(rev) - exp_rev) < 1
                status = "✅" if (qty_match and rev_match) else "❌"
                print(f"{warehouse[:45]:<45} {float(qty):>12,.0f} {exp_qty:>12,.0f} {status}")
                break
        else:
            print(f"{warehouse[:45]:<45} {float(qty):>12,.0f} {'—':>12} {'?':>3}")
    
    print("-" * 75)
    total_status = "✅" if abs(float(actual_total_rev) - expected_total[1]) < 1 else "❌"
    print(f"{'ИТОГО':<45} {float(actual_total_qty):>12,.0f} {expected_total[0]:>12,.0f} {total_status}")
    print(f"{'Выручка':<45} {float(actual_total_rev):>12,.0f} {expected_total[1]:>12,.0f}")
    
    return actual_total_qty, actual_total_rev


def main():
    print("\n🔌 Connecting to Roznica database...")
    
    conn = get_connection()
    cursor = conn.cursor()
    print("✅ Connection established!\n")
    
    # Step 1: Find nomenclature column
    nomen_col = find_nomenclature_ref_column(cursor)
    
    if not nomen_col:
        # Try common columns
        print("\nTrying known columns...")
        for col in ['_Fld53718RRef', '_Fld53719_RRRef', '_Fld53720RRef', '_Fld53716RRef']:
            try:
                query = f"""
                SELECT TOP 5
                    n._Description AS Product,
                    SUM(s.{QUANTITY_COL}) AS Qty
                FROM _AccumRg53715 s
                LEFT JOIN _Reference387 n ON s.{col} = n._IDRRef
                WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
                AND n._Description IS NOT NULL
                GROUP BY n._Description
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows and rows[0][0]:
                    print(f"✅ Found nomenclature in {col}")
                    nomen_col = col
                    break
            except Exception as e:
                continue
    
    if not nomen_col:
        print("❌ Could not find nomenclature reference column!")
        return
    
    # Step 2: Test nomenclature join
    test_nomenclature_join(cursor, nomen_col)
    
    # Step 3: Generate full report
    grand_qty, grand_rev = generate_full_report(cursor, nomen_col)
    
    # Step 4: Verify totals
    verify_totals(cursor, nomen_col)
    
    cursor.close()
    conn.close()
    print("\n🔌 Connection closed.")


if __name__ == "__main__":
    main()
