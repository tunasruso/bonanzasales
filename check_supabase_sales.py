
import requests
import json
import logging

SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"

def check_latest_sales():
    url = f"{SUPABASE_URL}/rest/v1/sales_analytics?select=sale_date,revenue&order=sale_date.desc&limit=5"
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
