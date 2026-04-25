# Ecostok2 Runbook

## Scope

This runbook covers:
- local raw sync from `172.16.0.45`
- raw table verification in `ecostok2_*`
- DM refresh execution on the Malaysia server

It does not cover:
- Modal production app changes
- Docker / Supabase restarts
- frontend code changes

## Paths

- Repo: `/home/aleksandr/bonanzasales`
- Local sync entrypoint: `/home/aleksandr/bonanzasales/local_ecostok2_sync.py`
- Malaysia refresh script target path: `/root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh`

## Environment

Required on the sync host:

```bash
export ECOSTOK2_SUPABASE_SERVICE_KEY='...'
```

Optional DB overrides:

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5444
export POSTGRES_USER=ecostock
export POSTGRES_DB=onec_ecostock_retail
```

## Standard sync commands

```bash
cd /home/aleksandr/bonanzasales

python3 local_sync_sales.py --date-to 2026-04-25 --days-back 3
python3 local_sync_visitors.py --date-to 2026-04-25 --days-back 3
python3 local_sync_inventory.py --date-to 2026-04-25 --days-back 3
```

Dry-run:

```bash
python3 local_sync_sales.py --date-to 2026-04-25 --days-back 3 --dry-run
python3 local_sync_visitors.py --date-to 2026-04-25 --days-back 3 --dry-run
python3 local_sync_inventory.py --date-to 2026-04-25 --days-back 3 --dry-run
```

## Validation after sync

### Sales

- verify `source rows == target rows`
- verify `source revenue == target revenue`

### Visitors

- verify rows per day exist in `ecostok2_visitors_analytics`
- verify `visitor_count` totals match source
- current day may legitimately be empty

### Inventory

- verify `insert_errors = 0`
- verify dedupe summary
- verify counts:

```bash
curl -s 'https://ecostok2.lidertex.cloud/rest/v1/ecostok2_inventory_analytics?snapshot_date=eq.2026-04-25&select=snapshot_date' \
  -H "apikey: $ECOSTOK2_SUPABASE_SERVICE_KEY" \
  -H "Authorization: Bearer $ECOSTOK2_SUPABASE_SERVICE_KEY" \
  -H 'Prefer: count=exact'
```

## Known incidents

### Inventory insert failed with `id` null

Symptom:
- HTTP `400`
- code `23502`
- message about `id` not-null violation

Meaning:
- `ecostok2_inventory_analytics.id` had no default/identity

Recovery:
- fix DB default on server
- rerun inventory sync only for affected dates

### Inventory insert failed with duplicate key

Symptom:
- HTTP `409`
- code `23505`
- unique constraint on `(store, product, snapshot_date, unit)`

Meaning:
- duplicates existed inside transformed payload

Recovery:
- dedupe before insert by:
  - `store`
  - `product`
  - `snapshot_date`
  - `unit`

### Visitors not visible in dashboard

Checks:
- confirm data exists in `ecostok2_visitors_analytics`
- confirm deployed JS bundle reads `ecostok2_visitors_analytics`
- confirm field name is `visitor_count`

## Refresh

Do not run from local host unless explicitly requested.

Malaysia server cron job:

```bash
flock -n /tmp/ecostok2_dm_refresh.lock /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

## Rollback / recovery

- Sales: rerun 3-day upsert window
- Visitors: rerun 3-day upsert window
- Inventory: rerun replace-by-date only for affected dates

## Git operations

```bash
cd /home/aleksandr/bonanzasales
git status --short
git add .
git commit -m "Add ecostok2 local sync and runbooks"
git push origin main
```
