# Ecostok2: Architecture and data pipeline

## 1. Overview

```text
1C (PostgreSQL)
   ↓
Local Sync (Python, 172.16.0.45)
   ↓
Supabase raw tables (ecostok2_*)
   ↓
DM tables/views (ecostok2_dm_*)
   ↓
Fast RPC (PostgREST)
   ↓
Frontend (https://ecostok2.lidertex.cloud)
```

## 2. Hosts and paths

- Local sync host: `172.16.0.45`
- Local repo path: `/home/aleksandr/bonanzasales`
- Local 1C PostgreSQL access: `127.0.0.1:5444`
- Self-hosted Supabase / frontend host: `ecostok2.lidertex.cloud`
- Current resolved public IP: `187.77.156.98`
- Malaysia server cron path: `/root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh`
- Malaysia server refresh log: `/root/bonanzasales2-ecostok2/logs/dm_refresh.log`

## 3. 1C data sources

- Sales: `_AccumRg53715`
- Inventory: `_AccumRg52568`
- Visitors: `_AccumRg53554`

## 4. Local sync on `172.16.0.45`

Primary entrypoint:
- [local_ecostok2_sync.py](/home/aleksandr/bonanzasales/local_ecostok2_sync.py:1)

Support files:
- [local_ecostok2_config.py](/home/aleksandr/bonanzasales/local_ecostok2_config.py:1)
- [local_ecostok2_supabase.py](/home/aleksandr/bonanzasales/local_ecostok2_supabase.py:1)

Thin wrappers:
- [local_sync_sales.py](/home/aleksandr/bonanzasales/local_sync_sales.py:1)
- [local_sync_inventory.py](/home/aleksandr/bonanzasales/local_sync_inventory.py:1)
- [local_sync_visitors.py](/home/aleksandr/bonanzasales/local_sync_visitors.py:1)

Current sync logic:

### Sales

- Sliding window: last 3 calendar days
- Target: `ecostok2_sales_analytics`
- Write mode: `UPSERT`
- Conflict key: `recorder_id`

### Inventory

- Sliding window: last 3 calendar days
- Target: `ecostok2_inventory_analytics`
- Write mode per day:
  - `DELETE WHERE snapshot_date = :date`
  - `INSERT` current snapshot for that day
- Pre-insert dedupe key:
  - `store + product + snapshot_date + unit`
- Dedupe rule:
  - `quantity = SUM(quantity)`
  - `product_group = first non-empty / most common`

### Visitors

- Sliding window: last 3 calendar days
- Target: `ecostok2_visitors_analytics`
- Write mode: `UPSERT`
- Conflict key: `(visit_date, store)`

## 5. Raw Supabase tables

- `ecostok2_sales_analytics`
- `ecostok2_inventory_analytics`
- `ecostok2_visitors_analytics`
- `ecostok2_product_weights`
- `ecostok2_product_classifications`

## 6. DM layer

- `ecostok2_dm_sales_day`
- `ecostok2_dm_sales_day_shop`
- `ecostok2_dm_sales_check_shop_category`
- `ecostok2_dm_sales_check_shop_product`
- `ecostok2_dm_inventory_current`
- `ecostok2_dm_product_weights_resolved`

Notes:
- Inventory is stored as current snapshot by `snapshot_date`
- Sales are kept in raw detail and rolled up by DM / RPC

## 7. Fast RPC

- `ecostok2_get_kpis_aggregated_fast`
- `ecostok2_get_shop_kpis_aggregated_fast`
- `ecostok2_get_pivot_aggregated_fast`
- `ecostok2_get_daily_dynamics_fast`

Expected latency:
- about `1-5 ms`

## 8. Frontend

- URL: `https://ecostok2.lidertex.cloud`

Current deployed bundle reads:
- sales: fast RPC / DM endpoints
- inventory: fast inventory path, not raw direct
- visitors: `ecostok2_visitors_analytics` directly with fields `visit_date, store, visitor_count`

## 9. Cron

Malaysia server refresh job:

```cron
*/30 * * * * flock -n /tmp/ecostok2_dm_refresh.lock /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh >> /root/bonanzasales2-ecostok2/logs/dm_refresh.log 2>&1
```

Purpose:
- refresh DM layer only
- does not pull data from 1C

## 10. System principles

- Do not join raw tables in frontend
- Do not run heavy analytical queries from frontend
- Keep calculations in Postgres
- Local sync writes raw tables only
- DM/RPC serve the application layer

## 11. Known limits

- Visitors for the current day may be absent
- Inventory is snapshot-based, not full change history
- No CDC from 1C, so sliding window `3 days` is used

## 12. Control points

- raw vs DM diff = `0`
- DM vs fast RPC diff = `0`
- latency < `5 ms`

## 13. Common commands

From `/home/aleksandr/bonanzasales`:

```bash
export ECOSTOK2_SUPABASE_SERVICE_KEY='...'
python3 local_ecostok2_sync.py --target sales --date-to 2026-04-25 --days-back 3
python3 local_ecostok2_sync.py --target visitors --date-to 2026-04-25 --days-back 3
python3 local_ecostok2_sync.py --target inventory --date-to 2026-04-25 --days-back 3
```
