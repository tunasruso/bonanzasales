#!/usr/bin/env python3
"""
1C:Retail 3.0 Sales Data Extraction Script (VERIFIED)
=====================================================
Extracts December 2025 revenue for Bolshevikov store from MS SQL

Key findings:
- Warehouse reference column: _Fld53725RRef (NOT _Fld53717RRef!)
- Revenue column: _Fld53732
- Quantity column: _Fld53731
- Date offset: +2000 years (Dec 2025 → Dec 4025)
"""

import pymssql
from decimal import Decimal
from typing import List, Tuple

# Database connection parameters
DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

# Key column mappings (verified)
WAREHOUSE_REF_COLUMN = '_Fld53725RRef'  # Reference to warehouse/store
REVENUE_COLUMN = '_Fld53732'            # Sum/Revenue
QUANTITY_COLUMN = '_Fld53731'           # Quantity


def get_connection():
    """Establish connection to MS SQL database."""
    return pymssql.connect(**DB_CONFIG)


def get_store_revenue(cursor, store_pattern: str, start_date: str, end_date: str) -> Tuple[str, Decimal, int]:
    """
    Calculate revenue for a store matching the pattern.
    
    Args:
        cursor: Database cursor
        store_pattern: SQL LIKE pattern for store name (e.g., '%Большевик%')
        start_date: Start date in 1C format (e.g., '4025-12-01')
        end_date: End date in 1C format (exclusive)
    
    Returns:
        Tuple of (store_name, revenue, record_count)
    """
    query = f"""
    SELECT 
        SUM(s.{REVENUE_COLUMN}) AS Revenue,
        COUNT(*) AS Records
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF_COLUMN} = w._IDRRef
    WHERE s._Period >= %s AND s._Period < %s
    AND (w._Description LIKE %s 
         OR w._ParentIDRRef IN (
             SELECT _IDRRef FROM _Reference640 
             WHERE _Description LIKE %s
         ))
    """
    
    cursor.execute(query, (start_date, end_date, store_pattern, store_pattern))
    row = cursor.fetchone()
    
    return (store_pattern.replace('%', ''), row[0] or Decimal(0), row[1] or 0)


def get_store_breakdown(cursor, store_pattern: str, start_date: str, end_date: str) -> List[Tuple]:
    """
    Get revenue breakdown by individual warehouse.
    
    Returns:
        List of tuples: (warehouse_name, quantity, revenue, records)
    """
    query = f"""
    SELECT 
        w._Description AS Warehouse,
        SUM(s.{QUANTITY_COLUMN}) AS Quantity,
        SUM(s.{REVENUE_COLUMN}) AS Revenue,
        COUNT(*) AS Records
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s.{WAREHOUSE_REF_COLUMN} = w._IDRRef
    WHERE s._Period >= %s AND s._Period < %s
    AND (w._Description LIKE %s 
         OR w._ParentIDRRef IN (
             SELECT _IDRRef FROM _Reference640 
             WHERE _Description LIKE %s
         ))
    GROUP BY w._Description
    ORDER BY Revenue DESC
    """
    
    cursor.execute(query, (start_date, end_date, store_pattern, store_pattern))
    return cursor.fetchall()


def generate_report(store_name: str, revenue: Decimal, target: float = None):
    """Generate formatted report table."""
    print("\n")
    print("╔" + "═" * 50 + "╗")
    print(f"║ {'Магазин':<22} │ {'Выручка за Декабрь 2025':>24} ║")
    print("╠" + "═" * 23 + "╪" + "═" * 26 + "╣")
    print(f"║ {store_name:<22} │ {float(revenue):>20,.2f} ₽ ║")
    print("╚" + "═" * 50 + "╝")
    
    if target:
        diff = abs(float(revenue) - target)
        pct = (diff / target) * 100
        
        print(f"\n📊 Verification:")
        print(f"   Target:     {target:>15,.2f} ₽")
        print(f"   Calculated: {float(revenue):>15,.2f} ₽")
        print(f"   Difference: {diff:>15,.2f} ₽ ({pct:.4f}%)")
        
        if pct < 0.01:
            print("\n✅ SUCCESS: Revenue matches target exactly!")
        elif pct < 1:
            print("\n✅ SUCCESS: Revenue matches target within 1% tolerance!")
        else:
            print("\n⚠️  WARNING: Revenue differs from target")


def main():
    """Main execution flow."""
    print("\n" + "═" * 70)
    print("  1C:Retail 3.0 - Sales Data Extraction")
    print("  Period: December 2025 | Store: Bolshevikov")
    print("═" * 70)
    
    # Configuration
    STORE_PATTERN = '%Большевик%'
    START_DATE = '4025-12-01'  # Dec 1, 2025 (+2000 years offset)
    END_DATE = '4026-01-01'   # Jan 1, 2026
    TARGET_REVENUE = 776661.00
    
    print("\n🔌 Connecting to Roznica database...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        print("✅ Connection established!")
        
        # Get breakdown by warehouse
        print("\n" + "-" * 70)
        print("Breakdown by warehouse:")
        print("-" * 70)
        print(f"{'Warehouse':<45} {'Quantity':<12} {'Revenue':<15}")
        print("-" * 70)
        
        breakdown = get_store_breakdown(cursor, '%Большевик%', START_DATE, END_DATE)
        
        total_qty = Decimal(0)
        total_rev = Decimal(0)
        
        for row in breakdown:
            wh_name, qty, rev, recs = row
            qty = qty or Decimal(0)
            rev = rev or Decimal(0)
            total_qty += qty
            total_rev += rev
            
            wh_display = wh_name[:44] if wh_name else "N/A"
            print(f"{wh_display:<45} {float(qty):>10,.2f} {float(rev):>13,.2f}")
        
        print("-" * 70)
        print(f"{'TOTAL':<45} {float(total_qty):>10,.2f} {float(total_rev):>13,.2f}")
        
        # Generate final report
        generate_report("Большевиков", total_rev, TARGET_REVENUE)
        
        cursor.close()
        conn.close()
        print("\n🔌 Connection closed.")
        
        return float(total_rev)
        
    except pymssql.Error as e:
        print(f"\n❌ Database error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
