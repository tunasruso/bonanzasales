import pymssql
import os

def get_connection():
    return pymssql.connect(
        server=os.getenv('MSSQL_HOST', '100.126.198.90'),
        user=os.getenv('MSSQL_USER', 'ai_bot'),
        password=os.getenv('MSSQL_PASSWORD', 'A8Ew}Glc'),
        database=os.getenv('MSSQL_DB', 'Roznica')
    )

def inspect_table(table_name):
    conn = get_connection()
    cursor = conn.cursor()
    
    print(f"--- Inspecting {table_name} ---")
    
    # Get total rows
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Total rows: {count}")
    
    # Get columns
    cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
    row = cursor.fetchone()
    
    if row:
        columns = [desc[0] for desc in cursor.description]
        print(f"Columns: {columns}")
        print(f"Sample row: {row}")
    else:
        print("Table is empty")
        # Get columns from schema
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        columns = [row[0] for row in cursor.fetchall()]
        print(f"Columns (from schema): {columns}")

    columns_to_check = [
        '_Fld52569RRef', '_Fld52570RRef', '_Fld52571RRef', '_Fld52572RRef', 
        '_Fld52573RRef', '_Fld52574RRef', '_Fld52577RRef'
    ]

    print("\n--- Checking All RRef Columns ---")
    
    for col in columns_to_check:
        print(f"Checking {col}...")
        
        # Check against Stores
        cursor.execute(f"""
            SELECT TOP 1 w._Description 
            FROM _AccumRg52568 s
            JOIN _Reference640 w ON s.{col} = w._IDRRef
        """)
        store_match = cursor.fetchone()
        if store_match:
            print(f"  -> MATCH Store (_Reference640): {store_match[0]}")
            
        # Check against Products
        cursor.execute(f"""
            SELECT TOP 1 n._Description 
            FROM _AccumRg52568 s
            JOIN _Reference387 n ON s.{col} = n._IDRRef
        """)
        prod_match = cursor.fetchone()
        if prod_match:
            print(f"  -> MATCH Product (_Reference387): {prod_match[0]}")

    conn.close()

if __name__ == "__main__":
    inspect_table("_AccumRg52568")
