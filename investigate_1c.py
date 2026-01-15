#!/usr/bin/env python3
"""
1C:Retail 3.0 - Diagnostic Script
Investigates the structure of sales register and its connections
"""

import pymssql
from decimal import Decimal

DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

def get_connection():
    return pymssql.connect(**DB_CONFIG)


def investigate_fld53717(cursor):
    """Investigate what _Fld53717RRef references."""
    print("=" * 70)
    print("INVESTIGATION: What does _Fld53717RRef reference?")
    print("=" * 70)
    
    # Get sample values from the register
    query = """
    SELECT TOP 10 _Fld53717RRef FROM (
        SELECT DISTINCT _Fld53717RRef
        FROM _AccumRg53715
        WHERE _Period >= '4025-12-01' AND _Period < '4026-01-01'
    ) AS sub
    """
    
    cursor.execute(query)
    sample_ids = cursor.fetchall()
    
    print(f"\nSample _Fld53717RRef values from sales register:")
    for i, row in enumerate(sample_ids):
        print(f"  {i+1}. {row[0].hex() if row[0] else 'NULL'}")
    
    # Check if these IDs exist in Reference640
    if sample_ids:
        placeholders = ', '.join(['%s'] * len(sample_ids))
        ids = [r[0] for r in sample_ids]
        
        query = f"""
        SELECT _IDRRef, _Description
        FROM _Reference640
        WHERE _IDRRef IN ({placeholders})
        """
        cursor.execute(query, ids)
        matches = cursor.fetchall()
        
        print(f"\nMatches found in Reference640: {len(matches)}")
        for m in matches:
            print(f"  - {m[1]}")
    
    return sample_ids


def find_all_warehouse_refs(cursor):
    """Find where Bolshevikov warehouses are referenced."""
    print("\n" + "=" * 70)
    print("Finding Bolshevikov warehouse references")
    print("=" * 70)
    
    # Get Bolshevikov IDs
    query = """
    SELECT _IDRRef, _Description
    FROM _Reference640
    WHERE _Description LIKE N'%Большевик%'
    """
    cursor.execute(query)
    warehouses = cursor.fetchall()
    
    for wh in warehouses:
        wh_id, wh_name = wh
        print(f"\n📍 {wh_name}")
        print(f"   ID: {wh_id.hex() if wh_id else 'NULL'}")
        
        # Check if this ID is referenced in _Fld53717RRef
        query = """
        SELECT COUNT(*) 
        FROM _AccumRg53715
        WHERE _Fld53717RRef = %s
        AND _Period >= '4025-12-01' AND _Period < '4026-01-01'
        """
        cursor.execute(query, (wh_id,))
        count = cursor.fetchone()[0]
        print(f"   Records in sales register (Dec 2025): {count}")


def explore_register_columns(cursor):
    """Explore all reference columns in the register."""
    print("\n" + "=" * 70)
    print("Exploring all reference columns in AccumRg53715")
    print("=" * 70)
    
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '_AccumRg53715'
    AND COLUMN_NAME LIKE '%RRef'
    ORDER BY COLUMN_NAME
    """
    cursor.execute(query)
    cols = cursor.fetchall()
    
    print("\nReference columns (RRef suffix):")
    for col in cols:
        print(f"  - {col[0]}")
    
    return [c[0] for c in cols]


def check_which_ref_is_warehouse(cursor, ref_columns):
    """Check which reference column contains warehouse data."""
    print("\n" + "=" * 70)
    print("Checking which RRef column links to warehouses")
    print("=" * 70)
    
    # Get Bolshevikov IDs
    query = """
    SELECT _IDRRef
    FROM _Reference640
    WHERE _Description LIKE N'%Большевик%'
    """
    cursor.execute(query)
    bolsh_ids = [r[0] for r in cursor.fetchall()]
    
    if not bolsh_ids:
        print("No Bolshevikov warehouses found!")
        return
    
    placeholders = ', '.join(['%s'] * len(bolsh_ids))
    
    for col in ref_columns:
        query = f"""
        SELECT COUNT(*) 
        FROM _AccumRg53715
        WHERE {col} IN ({placeholders})
        AND _Period >= '4025-12-01' AND _Period < '4026-01-01'
        """
        try:
            cursor.execute(query, bolsh_ids)
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"✅ {col}: {count} records match Bolshevikov!")
            else:
                print(f"   {col}: 0 matches")
        except Exception as e:
            print(f"   {col}: Error - {e}")


def join_investigation(cursor):
    """Try direct join to see actual warehouse names in sales."""
    print("\n" + "=" * 70)
    print("Direct JOIN: Sales → Reference640")
    print("=" * 70)
    
    query = """
    SELECT TOP 20
        w._Description AS Warehouse,
        SUM(s._Fld53732) AS Revenue,
        COUNT(*) AS Records
    FROM _AccumRg53715 s
    INNER JOIN _Reference640 w ON s._Fld53717RRef = w._IDRRef
    WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
    GROUP BY w._Description
    ORDER BY Revenue DESC
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"\nTop warehouses by revenue (December 2025):")
    print("-" * 70)
    for row in rows:
        wh, rev, rec = row
        if wh:
            print(f"  {wh[:50]:<50} {float(rev or 0):>12,.2f}")


def try_different_references(cursor):
    """Try different reference columns."""
    print("\n" + "=" * 70)
    print("Trying different reference columns for warehouse join")
    print("=" * 70)
    
    # List of potential warehouse reference columns
    ref_cols_to_try = ['_Fld53717RRef', '_Fld53718RRef', '_Fld53719RRef', 
                       '_Fld53720RRef', '_Fld53721RRef', '_Fld53722RRef']
    
    for col in ref_cols_to_try:
        print(f"\n📌 Trying column: {col}")
        
        query = f"""
        SELECT TOP 10
            w._Description AS Warehouse,
            SUM(s._Fld53732) AS Revenue
        FROM _AccumRg53715 s
        LEFT JOIN _Reference640 w ON s.{col} = w._IDRRef
        WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
        GROUP BY w._Description
        ORDER BY Revenue DESC
        """
        
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                wh, rev = row
                wh_name = wh[:40] if wh else "(NULL - unknown ref)"
                print(f"    {wh_name:<40} {float(rev or 0):>12,.2f}")
        except Exception as e:
            print(f"    Error: {e}")


def search_bolshevikov_everywhere(cursor):
    """Search for Bolshevikov in all joined tables."""
    print("\n" + "=" * 70)
    print("Comprehensive search: Bolshevikov in all reference joins")
    print("=" * 70)
    
    # Try joining on each RRef column and looking for Bolshevikov
    ref_cols = ['_Fld53717RRef', '_Fld53718RRef', '_Fld53719RRef', 
                '_Fld53720RRef', '_Fld53721RRef', '_Fld53722RRef',
                '_Fld53723RRef', '_Fld53724RRef', '_Fld53725RRef']
    
    for col in ref_cols:
        query = f"""
        SELECT 
            COUNT(*) as cnt,
            SUM(s._Fld53732) AS Revenue
        FROM _AccumRg53715 s
        INNER JOIN _Reference640 w ON s.{col} = w._IDRRef
        WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
        AND (w._Description LIKE N'%Большевик%' 
             OR w._ParentIDRRef IN (
                 SELECT _IDRRef FROM _Reference640 
                 WHERE _Description LIKE N'%Большевик%'
             ))
        """
        
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            if row[0] > 0:
                print(f"✅ {col}: {row[0]} records, Revenue: {float(row[1] or 0):,.2f} RUB")
            else:
                print(f"   {col}: 0 records")
        except Exception as e:
            print(f"   {col}: Error")


def main():
    print("\n🔌 Connecting to Roznica database...")
    
    conn = get_connection()
    cursor = conn.cursor()
    print("✅ Connection established!\n")
    
    # Step 1: Investigate _Fld53717RRef
    investigate_fld53717(cursor)
    
    # Step 2: Find Bolshevikov references
    find_all_warehouse_refs(cursor)
    
    # Step 3: Explore all reference columns
    ref_columns = explore_register_columns(cursor)
    
    # Step 4: Check which column links to warehouses
    check_which_ref_is_warehouse(cursor, ref_columns)
    
    # Step 5: Direct JOIN investigation
    join_investigation(cursor)
    
    # Step 6: Try different reference columns
    try_different_references(cursor)
    
    # Step 7: Search everywhere
    search_bolshevikov_everywhere(cursor)
    
    cursor.close()
    conn.close()
    print("\n🔌 Connection closed.")


if __name__ == "__main__":
    main()
