import pymssql

# 1C MS SQL Database
DB_CONFIG = {
    'server': '100.126.198.90',
    'user': 'ai_bot',
    'password': 'A8Ew}Glc',
    'database': 'Roznica'
}

def check_units():
    print("Connecting to database...")
    try:
        conn = pymssql.connect(**DB_CONFIG)
        cursor = conn.cursor(as_dict=True)
        
        query = """
        SELECT TOP 50
            n._Description AS product,
            u._Description AS unit,
            s._Fld53731 AS quantity,
            s._Fld53732 AS revenue
        FROM _AccumRg53715 s
        LEFT JOIN _Reference387 n ON s._Fld53716RRef = n._IDRRef -- Nomenclature
        LEFT JOIN _Reference188 u ON n._Fld9817RRef = u._IDRRef -- Unit
        WHERE s._Fld53732 > 0 -- Only positive revenue
        ORDER BY s._Period DESC
        """
        
        print("Fetching latest 50 sales records...")
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print(f"{'Product':<30} | {'Unit':<5} | {'Qty':<8} | {'Rev':<10} | {'Price/Unit':<10}")
        print("-" * 80)
        
        for row in rows:
            prod = row['product'] if row['product'] else "None"
            unit = row['unit'] if row['unit'] else "None"
            qty = float(row['quantity'])
            rev = float(row['revenue'])
            price = rev / qty if qty > 0 else 0
            
            print(f"{prod[:30]:<30} | {unit:<5} | {qty:<8.3f} | {rev:<10.2f} | {price:<10.2f}")
            
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_units()
