import pymssql
import os

# Connection details
server = "100.126.198.90"
user = "ai_bot"
password = "A8Ew}Glc"
database = "Roznica"

def debug_djemper():
    conn = pymssql.connect(server, user, password, database)
    cursor = conn.cursor(as_dict=True)

    print("Investigating 'Джемпер.зима' in 'Магазин (Большевиков) Торговый зал'...")

    # 1. Find the RRefs for Store and Product to be precise
    # Searching for Store
    cursor.execute("SELECT _IDRRef, _Description FROM _Reference640 WHERE _Description LIKE N'%Большевиков%Торговый зал%'")
    store = cursor.fetchone()
    if not store:
        print("Store not found!")
        return
    print(f"Store: {store['_Description']} (ID: {store['_IDRRef'].hex()})")
    store_id = store['_IDRRef']

    # Searching for ALL Products with exact or close name
    cursor.execute("SELECT _IDRRef, _Description, _Code FROM _Reference387 WHERE _Description LIKE N'%Джемпер.зима%'")
    products = cursor.fetchall()
    
    total_aggregate_balance = 0
    print(f"Found {len(products)} product variations with name 'Джемпер.зима'")

    for prod in products:
        prod_id = prod['_IDRRef']
        
        # 2. Inspect Raw Moves in _AccumRg52568
        query = f"""
        SELECT 
            _Period,
            _RecordKind,
            _Fld52575 AS Quantity,
            _Active
        FROM _AccumRg52568
        WHERE _Fld52573RRef = %s -- Store
          AND _Fld52570RRef = %s -- Product
        """
        cursor.execute(query, (store_id, prod_id))
        rows = cursor.fetchall()

        if not rows:
            continue

        balance = 0
        for row in rows:
            if row['_Active'] == b'\x01':
                qty = row['Quantity']
                if row['_RecordKind'] == 0:
                    balance += qty
                else:
                    balance -= qty
        
        print(f"  Prod: {prod['_Description']} (Code: {prod['_Code']}) -> Bal: {balance}")
        total_aggregate_balance += balance

    print(f"\nTotal Aggregate Balance across all 'Джемпер.зима' SKUs: {total_aggregate_balance}")
    conn.close()

if __name__ == "__main__":
    debug_djemper()
