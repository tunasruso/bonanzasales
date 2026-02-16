import pandas as pd
import json

file_path = '/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/dashboard.xlsx'

try:
    # Read Excel
    df = pd.read_excel(file_path, header=None)
    
    # Get all content
    data = df.values.tolist()
    
    # Save as JSON for easy reading
    with open('/Users/tunasruso/Documents/Antigravity/StasSales1CBackEnd/dashboard_structure.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print("Excel parsed successfully to dashboard_structure.json")
except Exception as e:
    print(f"Error: {e}")
