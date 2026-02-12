import psycopg2
import os
import sys
from decimal import Decimal
from datetime import datetime

# Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'dbname': os.getenv('POSTGRES_DB', 'Roznica'),
    'port': os.getenv('POSTGRES_PORT', 5432)
}

# Verified Columns
WAREHOUSE_REF = '_Fld53725RRef'
NOMENCLATURE_REF = '_Fld53716RRef' # From sales_daily_groups.py
REVENUE_COL = '_Fld53732'
QUANTITY_COL = '_Fld53731'

def check_sales():
    print(f"Checking sales for TODAY: {datetime.now().strftime('%Y-%m-%d')}")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Query for today's sales (Feb 10, 2026)
        query = f"""
        SELECT 
            w._Description as Store,
            n._Description as Product,
            SUM(s.{QUANTITY_COL}) as Quantity,
            SUM(s.{REVENUE_COL}) as Revenue
        FROM _AccumRg53715 s
        LEFT JOIN _Reference640 w ON s.{WAREHOUSE_REF} = w._IDRRef
        LEFT JOIN _Reference387 n ON s.{NOMENCLATURE_REF} = n._IDRRef
        WHERE s._Period >= '2026-02-10 00:00:00' 
          AND s._Period <= '2026-02-10 23:59:59'
        GROUP BY w._Description, n._Description
        ORDER BY Revenue DESC
        LIMIT 20
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("❌ No sales found for today (2026-02-10).")
        else:
            print(f"✅ Found {len(rows)} sales records! Top 20:")
            print(f"{'Store':<30} {'Product':<40} {'Qty':>10} {'Revenue':>12}")
            print("-" * 100)
            total_rev = Decimal(0)
            for row in rows:
                store = row[0][:28] if row[0] else "N/A"
                prod = row[1][:38] if row[1] else "N/A"
                qty = row[2] or 0
                rev = row[3] or 0
                total_rev += rev
                print(f"{store:<30} {prod:<40} {qty:>10.2f} {rev:>12.2f}")
            print("-" * 100)
            
            # Get total for the day
            cursor.execute(f"""
                SELECT SUM({REVENUE_COL}) 
                FROM _AccumRg53715 
                WHERE _Period >= '2026-02-10 00:00:00' 
                  AND _Period <= '2026-02-10 23:59:59'
            """)
            grand_total = cursor.fetchone()[0]
            print(f"💰 Total Daily Revenue: {grand_total:,.2f}")

        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_sales()
