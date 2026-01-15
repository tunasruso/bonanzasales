#!/usr/bin/env python3
"""Check for any KG sales"""
import pymssql

DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

conn = pymssql.connect(**DB_CONFIG)
cursor = conn.cursor()

print("=" * 70)
print("Looking for KG sales via unit of measure join")
print("=" * 70)

# Join sales -> nomenclature -> unit of measure
query = """
SELECT 
    u._Description AS Unit,
    COUNT(*) AS SalesCount,
    SUM(s._Fld53731) AS TotalQty,
    SUM(s._Fld53732) AS TotalRevenue
FROM _AccumRg53715 s
INNER JOIN _Reference387 n ON s._Fld53716RRef = n._IDRRef
LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
WHERE s._Period >= '4025-12-01' AND s._Period < '4026-01-01'
GROUP BY u._Description
ORDER BY TotalRevenue DESC
"""
cursor.execute(query)
rows = cursor.fetchall()

print(f"\nSales by unit of measure:")
print("-" * 70)
print(f"{'Unit':<30} {'Sales':<10} {'Quantity':<15} {'Revenue':<15}")
print("-" * 70)

for row in rows:
    unit = row[0] if row[0] else "(NULL/empty)"
    sales = row[1] or 0
    qty = row[2] or 0
    rev = row[3] or 0
    marker = "*** KG ***" if unit and 'кг' in unit.lower() else ""
    print(f"{unit:<30} {sales:<10} {float(qty):>13,.2f} {float(rev):>13,.2f} {marker}")

print("\n" + "=" * 70)
print("Looking for ANY kg data in the entire database (not just December)")
print("=" * 70)

query = """
SELECT TOP 20
    n._Description AS Product,
    u._Description AS Unit,
    SUM(s._Fld53731) AS TotalQty,
    SUM(s._Fld53732) AS TotalRevenue
FROM _AccumRg53715 s
INNER JOIN _Reference387 n ON s._Fld53716RRef = n._IDRRef
LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef
WHERE u._Description LIKE N'%кг%'
GROUP BY n._Description, u._Description
ORDER BY TotalRevenue DESC
"""
cursor.execute(query)
rows = cursor.fetchall()

if rows:
    print(f"\nProducts with KG unit (all time):")
    for row in rows:
        print(f"  {row[0][:40]}: {row[2]} {row[1]}, Revenue: {row[3]}")
else:
    print("\n❌ No products with 'кг' unit found in any sales data!")
    print("   This appears to be a clothing/textile store - all products are sold in pieces (шт)")

cursor.close()
conn.close()
