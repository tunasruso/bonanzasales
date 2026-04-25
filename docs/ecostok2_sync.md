# Ecostok2 Sync Guide

## Files

- [local_ecostok2_sync.py](/home/aleksandr/bonanzasales/local_ecostok2_sync.py:1)
- [local_ecostok2_config.py](/home/aleksandr/bonanzasales/local_ecostok2_config.py:1)
- [local_ecostok2_supabase.py](/home/aleksandr/bonanzasales/local_ecostok2_supabase.py:1)
- [local_sync_sales.py](/home/aleksandr/bonanzasales/local_sync_sales.py:1)
- [local_sync_inventory.py](/home/aleksandr/bonanzasales/local_sync_inventory.py:1)
- [local_sync_visitors.py](/home/aleksandr/bonanzasales/local_sync_visitors.py:1)
- [refresh_ecostok2_dm_last3days.sh](/home/aleksandr/bonanzasales/refresh_ecostok2_dm_last3days.sh:1)

## Runtime model

- `sales`: 3-day sliding window, upsert by `recorder_id`
- `visitors`: 3-day sliding window, upsert by `(visit_date, store)`
- `inventory`: 3-day sliding window, replace snapshot by date

## Real commands

### Sales

```bash
cd /home/aleksandr/bonanzasales
export ECOSTOK2_SUPABASE_SERVICE_KEY='...'
python3 local_sync_sales.py --date-to 2026-04-25 --days-back 3
```

### Visitors

```bash
cd /home/aleksandr/bonanzasales
export ECOSTOK2_SUPABASE_SERVICE_KEY='...'
python3 local_sync_visitors.py --date-to 2026-04-25 --days-back 3
```

### Inventory

```bash
cd /home/aleksandr/bonanzasales
export ECOSTOK2_SUPABASE_SERVICE_KEY='...'
python3 local_sync_inventory.py --date-to 2026-04-25 --days-back 3
```

### Dry-run

```bash
python3 local_sync_sales.py --date-to 2026-04-25 --days-back 3 --dry-run
python3 local_sync_visitors.py --date-to 2026-04-25 --days-back 3 --dry-run
python3 local_sync_inventory.py --date-to 2026-04-25 --days-back 3 --dry-run
```

## Inventory dedupe

Pre-insert aggregation key:

```text
store + product + snapshot_date + unit
```

Rules:
- sum `quantity`
- keep `store`, `product`, `snapshot_date`, `unit` as-is
- choose `product_group` as first non-empty / most common

## Diagnostics

### Check deployed frontend bundle

```bash
python3 - <<'PY'
import requests
html = requests.get('https://ecostok2.lidertex.cloud', timeout=30).text
print(html)
PY
```

### Check visitors API

```bash
curl -s 'https://ecostok2.lidertex.cloud/rest/v1/ecostok2_visitors_analytics?visit_date=eq.2026-04-24&select=visit_date,store,visitor_count' \
  -H "apikey: $ECOSTOK2_SUPABASE_SERVICE_KEY" \
  -H "Authorization: Bearer $ECOSTOK2_SUPABASE_SERVICE_KEY"
```

### Check inventory counts

```bash
for d in 2026-04-23 2026-04-24 2026-04-25; do
  curl -s "https://ecostok2.lidertex.cloud/rest/v1/ecostok2_inventory_analytics?snapshot_date=eq.${d}&select=snapshot_date" \
    -H "apikey: $ECOSTOK2_SUPABASE_SERVICE_KEY" \
    -H "Authorization: Bearer $ECOSTOK2_SUPABASE_SERVICE_KEY" \
    -H "Prefer: count=exact"
done
```
