import requests
import json
import logging
import os

SUPABASE_URL = os.getenv('SUPABASE_URL', 'http://127.0.0.1:8100')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY')
if not SUPABASE_KEY:
    raise RuntimeError('SUPABASE_SERVICE_KEY is required')

def check_latest_sales():
    url = f"{SUPABASE_URL}/rest/v1/ecostok2_sales_analytics?select=sale_date,revenue&order=sale_date.desc&limit=5"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                print("Latest 5 sales records in Supabase:")
                for r in data:
                    print(f"Date: {r['sale_date']}, Revenue: {r['revenue']}")
            else:
                print("No sales records found in Supabase.")
        else:
            print(f"Error checking Supabase: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exception checking Supabase: {e}")

if __name__ == "__main__":
    check_latest_sales()
