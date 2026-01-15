import pymssql

# MSSQL
server = "100.126.198.90"
user = "ai_bot"
password = "A8Ew}Glc"
database = "Roznica"

def check_data():
    # 2. Check 1C Store Duplicates
    print("\nChecking 1C Stores for duplicates...")
    try:
        conn = pymssql.connect(server, user, password, database)
        cursor = conn.cursor(as_dict=True)
        
        cursor.execute("SELECT _IDRRef, _Description FROM _Reference640 WHERE _Description LIKE N'%Большевиков%Торговый зал%'")
        rows = cursor.fetchall()
        print(f"Found {len(rows)} stores matching 'Большевиков...Торговый зал':")
        for r in rows:
            print(f"  ID: {r['_IDRRef'].hex()} | Name: {r['_Description']}")

        # 3. Check for Cartesian issues in Accum Register JOIN
        print("\nChecking for potential join multiplier...")
        # Get the first store ID found
        if rows:
            store_id = rows[0]['_IDRRef']
            store_name = rows[0]['_Description']
            
            # Find product ID
            cursor.execute("SELECT TOP 1 _IDRRef FROM _Reference387 WHERE _Description LIKE N'%Джемпер.зима%'")
            prod_row = cursor.fetchone()
            if prod_row:
                prod_id = prod_row['_IDRRef']
                
                # Count raw rows in AccumRg
                cursor.execute("SELECT COUNT(*) as cnt FROM _AccumRg52568 WHERE _Fld52573RRef = %s AND _Fld52570RRef = %s AND _Active = 1", (store_id, prod_id))
                raw_count = cursor.fetchone()['cnt']
                print(f"Raw Active Rows in AccumRg for {store_name}: {raw_count}")
                
            else:
                print("Product not found for join check")
            
        conn.close()
    except Exception as e:
        print(f"MSSQL Error: {e}")

    # 2. Check 1C Store Duplicates
    print("\nChecking 1C Stores for duplicates...")
    try:
        conn = pymssql.connect(server, user, password, database)
        cursor = conn.cursor(as_dict=True)
        
        cursor.execute("SELECT _IDRRef, _Description FROM _Reference640 WHERE _Description LIKE N'%Большевиков%Торговый зал%'")
        rows = cursor.fetchall()
        print(f"Found {len(rows)} stores matching 'Большевиков...Торговый зал':")
        for r in rows:
            print(f"  ID: {r['_IDRRef'].hex()} | Name: {r['_Description']}")
            
        conn.close()
    except Exception as e:
        print(f"MSSQL Error: {e}")

if __name__ == "__main__":
    check_data()
