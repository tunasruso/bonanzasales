import requests
import json
import os

# Supabase Config
SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

def analyze_products():
    print("Fetching distinct product groups and names...", flush=True)
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    # 1. Fetch distinct product groups
    url_groups = f"{SUPABASE_URL}/rest/v1/sales_analytics?select=product_group"
    # We'll use a script-based distinct approach as Supabase REST doesn't do distinct easily without RPC, 
    # but we can fetch a sample or use a hack. Actually, let's just fetch a chunk and parse in python.
    # Or better: Create a small set of all seen names.
    
    # Let's fetch last 2000 records to get a good representative sample
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics?select=product,product_group&limit=2000&order=sale_date.desc"
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        
        groups = set()
        products = set()
        
        sample_map = {} # group -> list of sample products
        
        for row in data:
            g = row.get('product_group')
            p = row.get('product')
            if g: groups.add(g)
            if p: products.add(p)
            
            if g and p:
                if g not in sample_map:
                    sample_map[g] = set()
                if len(sample_map[g]) < 5:
                    sample_map[g].add(p)
        
        print("\n=== Found Product Groups ===")
        for g in sorted(groups):
            print(f"[{g}]")
            for p in sample_map.get(g, []):
                print(f"  - {p}")
            print()
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_products()
