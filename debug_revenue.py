import requests
import json
from collections import Counter

SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

def check_date(date_str):
    print(f"Checking data for {date_str}...")
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Fetch all rows for this date
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics?sale_date=eq.{date_str}&select=*"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error: {response.text}")
        return

    data = response.json()
    print(f"Total records found: {len(data)}")
    
    total_revenue = sum(r['revenue'] for r in data)
    print(f"Total Revenue: {total_revenue:,.2f}")
    
    # Check for duplicates based on recorder_id
    recorder_ids = [r['recorder_id'] for r in data]
    counts = Counter(recorder_ids)
    
    duplicates = {k: v for k, v in counts.items() if v > 1}
    
    if duplicates:
        print(f"\nFOUND {len(duplicates)} DUPLICATE RECORDER IDs!")
        print(f"Example duplicates (id: count):")
        for k, v in list(duplicates.items())[:5]:
            print(f"  {k}: {v}")
            
        # Show detail of one duplicate
        dup_id = list(duplicates.keys())[0]
        print(f"\nDetails for duplicate ID {dup_id}:")
        dup_rows = [r for r in data if r['recorder_id'] == dup_id]
        for row in dup_rows:
            print(f"  product={row['product']}, qty={row['quantity']}, rev={row['revenue']}, created_at={row.get('created_at', 'N/A')}")
    else:
        print("\nNo duplicate recorder_ids found.")

if __name__ == "__main__":
    check_date("2026-01-02")
