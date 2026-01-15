#!/usr/bin/env python3
"""
Investigate categories and units of measure structure
"""

import pymssql

DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

def get_connection():
    return pymssql.connect(**DB_CONFIG)

def main():
    conn = get_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("1. Investigating Nomenclature (_Reference387) structure")
    print("=" * 70)
    
    query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '_Reference387'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  {row[0]:<30} {row[1]}")
    
    print("\n" + "=" * 70)
    print("2. Sample nomenclature data with parent")
    print("=" * 70)
    
    query = """
    SELECT TOP 20
        n._Description AS Product,
        p._Description AS Parent
    FROM _Reference387 n
    LEFT JOIN _Reference387 p ON n._ParentIDRRef = p._IDRRef
    WHERE n._Description IS NOT NULL
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  {row[0][:40]:<40} -> {row[1] if row[1] else '(ROOT)'}")
    
    print("\n" + "=" * 70)
    print("3. Investigating Categories (_Reference271) structure")
    print("=" * 70)
    
    query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '_Reference271'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  {row[0]:<30} {row[1]}")
    
    print("\n" + "=" * 70)
    print("4. Sample categories")
    print("=" * 70)
    
    query = """
    SELECT TOP 20 _Description
    FROM _Reference271
    WHERE _Description IS NOT NULL
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  {row[0]}")
    
    print("\n" + "=" * 70)
    print("5. Warehouse hierarchy (Store -> City)")
    print("=" * 70)
    
    query = """
    SELECT 
        w._Description AS Warehouse,
        p._Description AS Parent,
        g._Description AS Grandparent
    FROM _Reference640 w
    LEFT JOIN _Reference640 p ON w._ParentIDRRef = p._IDRRef
    LEFT JOIN _Reference640 g ON p._ParentIDRRef = g._IDRRef
    WHERE w._Description LIKE N'%Торговый зал%' 
       OR w._Description LIKE N'%СКЛАД%'
    ORDER BY w._Description
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        wh = row[0] if row[0] else "?"
        parent = row[1] if row[1] else "(NONE)"
        gp = row[2] if row[2] else "(NONE)"
        print(f"  {wh[:35]:<35} -> {parent[:20]:<20} -> {gp}")
    
    print("\n" + "=" * 70)
    print("6. Units of measure exploration")
    print("=" * 70)
    
    # Find units table
    query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME LIKE '%Reference188%' OR TABLE_NAME LIKE '%Единиц%'
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  Table: {row[0]}")
    
    # Sample from Reference188 (ЕдиницыИзмерения)
    query = """
    SELECT TOP 20 _Description
    FROM _Reference188
    WHERE _Description IS NOT NULL
    """
    cursor.execute(query)
    print("\nUnits of measure:")
    for row in cursor.fetchall():
        print(f"  {row[0]}")
    
    print("\n" + "=" * 70)
    print("7. Check if nomenclature has unit reference")
    print("=" * 70)
    
    query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '_Reference387'
    AND COLUMN_NAME LIKE '%RRef%'
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"  {row[0]}")
    
    cursor.close()
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
