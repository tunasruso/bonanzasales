import psycopg2
import requests
import os
from collections import defaultdict

# Supabase
SUPABASE_URL = "https://lyfznzntclgitarujlab.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx5Znpuem50Y2xnaXRhcnVqbGFiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MTM2OTgsImV4cCI6MjA2NzQ4OTY5OH0.UyUQzzKQ70p7RHw4TWHvUutMkGuo9VGZiGPdVZpVcs0"
headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

# 1C
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', '100.91.185.91'),
    user=os.getenv('POSTGRES_USER', 'ecostock'),
    password=os.getenv('POSTGRES_PASSWORD', 'Kd*2m5Th'),
    dbname=os.getenv('POSTGRES_DB', 'onec_ecostock_retail'),
    port=os.getenv('POSTGRES_PORT', 5444)
)
cursor = conn.cursor()

# Get 1C sales for Kolomna
cursor.execute("""
    WITH sales AS (
        SELECT 
            CAST(COALESCE(m._Description, w._Description) AS text) as store,
            CAST(n._Description AS text) as product_name,
            CAST(g._Description AS text) as product_group,
            SUM(s._Fld53732) as revenue
        FROM _AccumRg53715 s
        INNER JOIN _Reference640 w ON s._Fld53725RRef = w._IDRRef
        LEFT JOIN _Reference640 m ON w._ParentIDRRef = m._IDRRef
        LEFT JOIN _Reference387 n ON s._Fld53716RRef = n._IDRRef
        LEFT JOIN _Reference387 g ON n._ParentIDRRef = g._IDRRef
        WHERE s._Period >= '2026-02-01 00:00:00'
        AND s._Period <= '2026-02-24 23:59:59'
        GROUP BY 
            CAST(COALESCE(m._Description, w._Description) AS text),
            CAST(n._Description AS text),
            CAST(g._Description AS text)
    )
    SELECT product_group, product_name, revenue FROM sales WHERE store = 'Коломна'
""")
rows_1c = cursor.fetchall()

# Supabase sales for Kolomna
url = f"{SUPABASE_URL}/rest/v1/sales_analytics?sale_date=gte.2026-02-01&sale_date=lte.2026-02-24&store=eq.Коломна"
sb_sales = []
offset = 0; limit = 1000
while True:
    res = requests.get(f"{url}&offset={offset}&limit={limit}", headers=headers).json()
    sb_sales.extend(res)
    if len(res) < limit: break
    offset += limit

# Match logic in frontend
def is_new(pGroup, pName):
    pn = str(pName).lower()
    pg = str(pGroup).lower()
    kp_kw = ['кпб', 'пододеяльник', 'простын', 'наволоч', 'комплект постельного', 'полотен']
    if any(k in pg or k in pn for k in kp_kw):
        return True
    return False

# Aggregate 1C
onec_second_total = 0
onec_items = defaultdict(float)
for row in rows_1c:
    group = row[0] or ""
    name = row[1] or ""
    rev = float(row[2])
    if not is_new(group, name):
        onec_second_total += rev
        onec_items[(group, name)] += rev

# Aggregate Supabase
sb_second_total = 0
sb_items = defaultdict(float)
for row in sb_sales:
    group = row.get('product_group') or ""
    name = row.get('product') or ""
    rev = float(row.get('revenue') or 0.0)
    if not is_new(group, name):
        sb_second_total += rev
        sb_items[(group, name)] += rev

print(f"1C Second Total: {onec_second_total:.2f}")
print(f"SB Second Total: {sb_second_total:.2f}")

# Find differences
diffs = []
all_keys = set(onec_items.keys()).union(set(sb_items.keys()))
for k in all_keys:
    r1c = onec_items[k]
    rsb = sb_items[k]
    if abs(r1c - rsb) > 0.01:
        diffs.append((k[0], k[1], r1c, rsb, rsb - r1c))

if diffs:
    print("\nDifferences (Group | Product | 1C_Rev | SB_Rev | Diff(SB-1C)):")
    for d in sorted(diffs, key=lambda x: x[4], reverse=True):
         print(f"{d[0]:30s} | {d[1]:40s} | {d[2]:>10.2f} | {d[3]:>10.2f} | {d[4]:>10.2f}")
else:
    print("No item-level differences found!")
