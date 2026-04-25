# Ecostok2 Runbook

## Безопасность

Нельзя:

- трогать production `ecostok` таблицы без префикса;
- рестартовать Docker/Supabase/PostgREST для refresh;
- создавать новые Supabase stack/containers;
- запускать тяжелые raw sales queries из frontend.

Можно:

- работать с `public.ecostok2_*`;
- запускать `docker exec ... psql` внутри существующего контейнера `ecostok-supabase-db`;
- обновлять frontend dist ecostok2;
- reload nginx только при изменении nginx config.

## Проверить refresh cron

```bash
crontab -l
tail -80 /root/bonanzasales2-ecostok2/logs/dm_refresh.log
```

Ожидаемая cron-строка:

```cron
*/30 * * * * flock -n /tmp/ecostok2_dm_refresh.lock /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh >> /root/bonanzasales2-ecostok2/logs/dm_refresh.log 2>&1
```

## Ручной refresh DM

```bash
bash /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

Скрипт:

- сам определяет диапазон `max(sale_date)-2 .. max(sale_date)`;
- refresh-ит sales DM и `ecostok2_dm_inventory_current`;
- проверяет raw vs DM;
- при расхождениях завершает работу с `exit 1`.

## Проверка sales raw vs DM vs fast RPC

```bash
docker exec -i ecostok-supabase-db psql -U postgres -d postgres -c "
with days as (
  select generate_series(
    (select max(sale_date) - interval '2 days' from public.ecostok2_sales_analytics),
    (select max(sale_date) from public.ecostok2_sales_analytics),
    interval '1 day'
  )::date as sale_date
),
raw as (
  select
    sale_date,
    sum(revenue) raw_revenue,
    count(*) raw_positions,
    count(distinct coalesce(nullif(split_part(coalesce(recorder_id,''), '_', 1), ''), id::text)) raw_checks
  from public.ecostok2_sales_analytics
  where sale_date in (select sale_date from days)
  group by sale_date
),
fast as (
  select
    d.sale_date,
    f.total_revenue fast_revenue,
    f.total_positions fast_positions,
    f.total_checks fast_checks
  from days d
  cross join lateral public.ecostok2_get_kpis_aggregated_fast(d.sale_date, d.sale_date, null, null, null) f
)
select
  r.sale_date,
  r.raw_revenue,
  dm.total_revenue dm_revenue,
  f.fast_revenue,
  dm.total_revenue - r.raw_revenue dm_diff,
  f.fast_revenue - dm.total_revenue fast_diff,
  r.raw_positions,
  dm.total_positions dm_positions,
  f.fast_positions,
  r.raw_checks,
  dm.total_checks_day dm_checks,
  f.fast_checks
from raw r
join public.ecostok2_dm_sales_day dm on dm.sale_date = r.sale_date
join fast f on f.sale_date = r.sale_date
order by r.sale_date;
"
```

## Проверка inventory current

```bash
docker exec -i ecostok-supabase-db psql -U postgres -d postgres -c "
select 'raw_inventory' source, snapshot_date, count(*) rows, sum(quantity) qty
from public.ecostok2_inventory_analytics
where snapshot_date = (select max(snapshot_date) from public.ecostok2_inventory_analytics)
group by snapshot_date
union all
select 'dm_inventory_current', snapshot_date, count(*) rows, sum(quantity) qty
from public.ecostok2_dm_inventory_current
group by snapshot_date;
"
```

## Проверка visitors

Visitors не идут через DM. Frontend читает `ecostok2_visitors_analytics` напрямую.

```bash
docker exec -i ecostok-supabase-db psql -U postgres -d postgres -c "
select visit_date, count(*) stores, sum(visitor_count) visitors
from public.ecostok2_visitors_analytics
where visit_date between current_date - interval '7 days' and current_date
group by visit_date
order by visit_date;
"
```

## Проверка API

```bash
curl -I https://ecostok2.lidertex.cloud
```

Проверка fast RPC через REST:

```bash
curl -sS -X POST https://ecostok2.lidertex.cloud/rest/v1/rpc/ecostok2_get_kpis_aggregated_fast \
  -H "apikey: $SUPABASE_ANON_KEY" \
  -H "Authorization: Bearer $SUPABASE_ANON_KEY" \
  -H "Content-Type: application/json" \
  -d '{"p_start":"2026-04-25","p_end":"2026-04-25","p_stores":null,"p_groups":null,"p_products":null}'
```

## Frontend build/deploy

```bash
cd /root/bonanzasales2-ecostok2/sales-dashboard
npm run build
rsync -a --delete dist/ /opt/ecostok2-sales-dashboard/dist/
```

## Nginx

Config:

```text
/etc/nginx/sites-available/ecostok2.lidertex.cloud
```

Проверка:

```bash
nginx -t
systemctl reload nginx
```

## Rollback

Для refresh rollback обычно не нужен: скрипт пересчитывает последние 3 дня из raw. Если DM повреждены, повторить:

```bash
bash /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

Для frontend rollback: вернуть предыдущий commit и пересобрать `dist`.

