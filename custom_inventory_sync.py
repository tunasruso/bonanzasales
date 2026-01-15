import pymssql
import os
import requests
import json
from decimal import Decimal

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
    return pymssql.connect(
        server=os.getenv('MSSQL_HOST', '100.126.198.90'),
        user=os.getenv('MSSQL_USER', 'ai_bot'),
        password=os.getenv('MSSQL_PASSWORD', 'A8Ew}Glc'),
        database=os.getenv('MSSQL_DB', 'Roznica')
    )

def extract_inventory():
    print("Connecting to MS SQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Logic:
    # _AccumRg52568 is "GoodsInWarehouses" (from investigation)
    # _Fld52573RRef = Warehouse (Verified: Found "Магазин (Туристская)")
    # _Fld52570RRef = Product (Verified: Found "Аксессуары" etc)
    # _Fld52575 = Quantity (Verified: Found values like 250.000)
    # _RecordKind = 0 (Plus/Receipt), 1 (Minus/Expense)
    
    query = """
    SELECT 
        ISNULL(m._Description, w._Description) as Store,
        n._Description as Product,
        SUM(CASE WHEN s._RecordKind = 0 THEN s._Fld52575 ELSE -s._Fld52575 END) as Quantity,
        g._Description as ProductGroup
    FROM _AccumRg52568 s
    JOIN _Reference640 w ON s._Fld52573RRef = w._IDRRef -- Warehouse
    LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef -- Parent Store Group
    JOIN _Reference387 n ON s._Fld52570RRef = n._IDRRef -- Product
    LEFT JOIN _Reference387 g ON n._ParentIDRRef = g._IDRRef -- Product Group
    WHERE s._Active = 1
    GROUP BY ISNULL(m._Description, w._Description), n._Description, g._Description
    HAVING SUM(CASE WHEN s._RecordKind = 0 THEN s._Fld52575 ELSE -s._Fld52575 END) <> 0
    ORDER BY Store, n._Description
    """
    
    print("Executing query...")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(f"Extracted {len(rows)} inventory records.")
    
    inventory_data = []
    for row in rows:
        inventory_data.append({
            "store": row[0],
            "product": row[1],
            "quantity": float(row[2]),
            "product_group": row[3] if row[3] else "Unknown"
        })
        
    conn.close()
    return inventory_data

def upload_to_supabase(data):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    # 1. Truncate table (to replace full snapshot)
    # Supabase doesn't have a direct truncate via REST, checking row count first or just upserting?
    # Strategy: Delete all rows first for clean snapshot state.
    print("Clearing existing inventory...")
    delete_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=neq.0"
    requests.delete(delete_url, headers=headers)
    
    # 2. Upload in batches
    batch_size = 1000
    total_uploaded = 0
    
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
            print(f"Error uploading batch {i}: {response.text}")
        else:
            total_uploaded += len(batch)
            print(f"Uploaded {total_uploaded}/{len(data)} records")

if __name__ == "__main__":
    try:
        data = extract_inventory()
        if data:
            upload_to_supabase(data)
            print("Sync completed successfully.")
        else:
            print("No data found.")
    except Exception as e:
        print(f"Meta-Error: {e}")
