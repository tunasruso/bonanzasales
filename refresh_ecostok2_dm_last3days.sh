#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/root/bonanzasales2-ecostok2"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_HINT="/tmp/ecostok2_dm_refresh.lock"
PSQL=(docker exec -i ecostok-supabase-db psql -U postgres -d postgres)

mkdir -p "$LOG_DIR"

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

log "ecostok2 DM refresh started"
log "cron lock hint: $LOCK_HINT"

RANGE="$("${PSQL[@]}" -Atc "
select (max_sale_date - interval '2 days')::date || '|' || max_sale_date::date
from (
  select max(sale_date) as max_sale_date
  from public.ecostok2_sales_analytics
) s
where max_sale_date is not null;
")"

if [[ -z "$RANGE" || "$RANGE" != *"|"* ]]; then
  log "ERROR: cannot determine refresh range from public.ecostok2_sales_analytics"
  exit 1
fi

START_DATE="${RANGE%%|*}"
END_DATE="${RANGE##*|}"

log "refresh range: $START_DATE .. $END_DATE"

"${PSQL[@]}" -v ON_ERROR_STOP=1 -v start_date="$START_DATE" -v end_date="$END_DATE" <<'SQL'
begin;

create temp table tmp_ecostok2_product_pairs on commit drop as
select distinct
  coalesce(product_group, '') as product_group,
  coalesce(product, '') as product
from public.ecostok2_sales_analytics;

create temp table tmp_ecostok2_weight_candidates on commit drop as
select
  p.product_group,
  p.product,
  coalesce(w.category, 'second') as category,
  coalesce(w.avg_weight_kg, 0) as avg_weight_kg,
  row_number() over (
    partition by p.product_group, p.product
    order by
      case
        when w.product_group = p.product_group and nullif(w.product_name_pattern, '') is not null then 1
        when w.product_group = p.product_group and nullif(w.product_name_pattern, '') is null then 2
        when w.product_group in ('%', 'АКЦИЯ') and nullif(w.product_name_pattern, '') is not null then 3
        else 9
      end,
      length(coalesce(w.product_name_pattern, '')) desc,
      w.id
  ) as rn
from tmp_ecostok2_product_pairs p
join public.ecostok2_product_weights w
  on (
    w.product_group = p.product_group
    and (
      nullif(w.product_name_pattern, '') is null
      or position(w.product_name_pattern in p.product) > 0
    )
  )
  or (
    w.product_group in ('%', 'АКЦИЯ')
    and nullif(w.product_name_pattern, '') is not null
    and position(w.product_name_pattern in p.product) > 0
  );

delete from public.ecostok2_dm_product_weights_resolved;

insert into public.ecostok2_dm_product_weights_resolved (
  product_group,
  product,
  category,
  avg_weight_kg,
  is_kpb,
  is_aplus
)
select
  p.product_group,
  p.product,
  case
    when lower(p.product_group) like '%кпб%'
      or lower(p.product_group) like '%пододеяльник%'
      or lower(p.product_group) like '%простын%'
      or lower(p.product_group) like '%наволоч%'
      or lower(p.product_group) like '%комплект постельного%'
      or lower(p.product_group) like '%полотен%'
      or lower(p.product) like '%кпб%'
      or lower(p.product) like '%пододеяльник%'
      or lower(p.product) like '%простын%'
      or lower(p.product) like '%наволоч%'
      or lower(p.product) like '%комплект постельного%'
      or lower(p.product) like '%полотен%'
    then 'new'
    else coalesce(c.category, 'second')
  end as category,
  case
    when lower(p.product_group) like '%кпб%'
      or lower(p.product_group) like '%пододеяльник%'
      or lower(p.product_group) like '%простын%'
      or lower(p.product_group) like '%наволоч%'
      or lower(p.product_group) like '%комплект постельного%'
      or lower(p.product_group) like '%полотен%'
      or lower(p.product) like '%кпб%'
      or lower(p.product) like '%пододеяльник%'
      or lower(p.product) like '%простын%'
      or lower(p.product) like '%наволоч%'
      or lower(p.product) like '%комплект постельного%'
      or lower(p.product) like '%полотен%'
    then 0
    else coalesce(c.avg_weight_kg, 0)
  end as avg_weight_kg,
  case
    when lower(p.product_group) like '%кпб%'
      or lower(p.product_group) like '%пододеяльник%'
      or lower(p.product_group) like '%простын%'
      or lower(p.product_group) like '%наволоч%'
      or lower(p.product_group) like '%комплект постельного%'
      or lower(p.product_group) like '%полотен%'
      or lower(p.product) like '%кпб%'
      or lower(p.product) like '%пододеяльник%'
      or lower(p.product) like '%простын%'
      or lower(p.product) like '%наволоч%'
      or lower(p.product) like '%комплект постельного%'
      or lower(p.product) like '%полотен%'
    then true
    else coalesce(c.category, 'second') = 'new'
  end as is_kpb,
  p.product like '%A+%' or p.product like '%А+%' or p.product like '%А!%' or p.product like '%A!%' as is_aplus
from tmp_ecostok2_product_pairs p
left join tmp_ecostok2_weight_candidates c
  on c.product_group = p.product_group
 and c.product = p.product
 and c.rn = 1;

create temp table tmp_ecostok2_line_classified on commit drop as
select
  s.sale_date,
  coalesce(s.store, 'Не указано') as store,
  coalesce(s.product_group, 'Не указано') as product_group,
  coalesce(s.product, 'Не указано') as product,
  coalesce(nullif(split_part(coalesce(s.recorder_id, ''), '_', 1), ''), s.id::text) as check_id,
  coalesce(s.revenue, 0) as revenue,
  case
    when coalesce(m.category, 'second') = 'new' then 0
    else coalesce(s.quantity_pcs, 0) * coalesce(m.avg_weight_kg, 0)
  end as kg,
  coalesce(s.quantity_pcs, 0) as pcs,
  coalesce(m.category, 'second') as category,
  coalesce(m.is_aplus, false) as is_aplus,
  coalesce(m.is_kpb, false) as is_kpb
from public.ecostok2_sales_analytics s
left join public.ecostok2_dm_product_weights_resolved m
  on m.product_group = coalesce(s.product_group, '')
 and m.product = coalesce(s.product, '')
where s.sale_date between date :'start_date' and date :'end_date';

create index on tmp_ecostok2_line_classified (sale_date);
create index on tmp_ecostok2_line_classified (sale_date, store);
create index on tmp_ecostok2_line_classified (sale_date, store, product_group, check_id);
create index on tmp_ecostok2_line_classified (sale_date, store, product_group, product, check_id);

delete from public.ecostok2_dm_sales_day
where sale_date between date :'start_date' and date :'end_date';

insert into public.ecostok2_dm_sales_day (
  sale_date,
  total_revenue,
  total_kg,
  total_pcs,
  total_checks_day,
  total_positions,
  second_revenue,
  second_kg,
  second_pcs,
  second_checks_day,
  second_positions,
  new_revenue,
  new_pcs,
  new_checks_day,
  new_positions,
  aplus_revenue,
  aplus_kg,
  aplus_checks_day,
  bedding_revenue,
  bedding_checks_day
)
select
  sale_date,
  sum(revenue),
  sum(kg),
  sum(pcs),
  count(distinct check_id),
  count(*),
  coalesce(sum(revenue) filter (where category = 'second'), 0),
  coalesce(sum(kg) filter (where category = 'second'), 0),
  coalesce(sum(pcs) filter (where category = 'second'), 0),
  count(distinct check_id) filter (where category = 'second'),
  count(*) filter (where category = 'second'),
  coalesce(sum(revenue) filter (where category = 'new'), 0),
  coalesce(sum(pcs) filter (where category = 'new'), 0),
  count(distinct check_id) filter (where category = 'new'),
  count(*) filter (where category = 'new'),
  coalesce(sum(revenue) filter (where is_aplus), 0),
  coalesce(sum(kg) filter (where is_aplus), 0),
  count(distinct check_id) filter (where is_aplus),
  coalesce(sum(revenue) filter (where is_kpb), 0),
  count(distinct check_id) filter (where is_kpb)
from tmp_ecostok2_line_classified
group by sale_date;

delete from public.ecostok2_dm_sales_day_shop
where sale_date between date :'start_date' and date :'end_date';

insert into public.ecostok2_dm_sales_day_shop (
  sale_date,
  store,
  total_revenue,
  total_kg,
  total_pcs,
  total_checks_shop,
  total_positions,
  second_revenue,
  second_kg,
  second_pcs,
  second_checks_shop,
  second_positions,
  new_revenue,
  new_pcs,
  new_checks_shop,
  new_positions,
  aplus_revenue,
  aplus_kg,
  aplus_checks_shop,
  bedding_revenue,
  bedding_checks_shop
)
select
  sale_date,
  store,
  sum(revenue),
  sum(kg),
  sum(pcs),
  count(distinct check_id),
  count(*),
  coalesce(sum(revenue) filter (where category = 'second'), 0),
  coalesce(sum(kg) filter (where category = 'second'), 0),
  coalesce(sum(pcs) filter (where category = 'second'), 0),
  count(distinct check_id) filter (where category = 'second'),
  count(*) filter (where category = 'second'),
  coalesce(sum(revenue) filter (where category = 'new'), 0),
  coalesce(sum(pcs) filter (where category = 'new'), 0),
  count(distinct check_id) filter (where category = 'new'),
  count(*) filter (where category = 'new'),
  coalesce(sum(revenue) filter (where is_aplus), 0),
  coalesce(sum(kg) filter (where is_aplus), 0),
  count(distinct check_id) filter (where is_aplus),
  coalesce(sum(revenue) filter (where is_kpb), 0),
  count(distinct check_id) filter (where is_kpb)
from tmp_ecostok2_line_classified
group by sale_date, store;

delete from public.ecostok2_dm_sales_check_shop_category
where sale_date between date :'start_date' and date :'end_date';

insert into public.ecostok2_dm_sales_check_shop_category (
  sale_date,
  store,
  product_group,
  check_id,
  revenue,
  kg,
  pcs,
  positions,
  second_revenue,
  second_kg,
  second_pcs,
  second_positions,
  new_revenue,
  new_pcs,
  new_positions,
  aplus_revenue,
  aplus_kg,
  bedding_revenue
)
select
  sale_date,
  store,
  product_group,
  check_id,
  sum(revenue),
  sum(kg),
  sum(pcs),
  count(*),
  coalesce(sum(revenue) filter (where category = 'second'), 0),
  coalesce(sum(kg) filter (where category = 'second'), 0),
  coalesce(sum(pcs) filter (where category = 'second'), 0),
  count(*) filter (where category = 'second'),
  coalesce(sum(revenue) filter (where category = 'new'), 0),
  coalesce(sum(pcs) filter (where category = 'new'), 0),
  count(*) filter (where category = 'new'),
  coalesce(sum(revenue) filter (where is_aplus), 0),
  coalesce(sum(kg) filter (where is_aplus), 0),
  coalesce(sum(revenue) filter (where is_kpb), 0)
from tmp_ecostok2_line_classified
group by sale_date, store, product_group, check_id;

delete from public.ecostok2_dm_sales_check_shop_product
where sale_date between date :'start_date' and date :'end_date';

insert into public.ecostok2_dm_sales_check_shop_product (
  sale_date,
  store,
  product_group,
  product,
  check_id,
  revenue,
  kg,
  pcs,
  positions,
  second_revenue,
  second_kg,
  second_pcs,
  second_positions,
  new_revenue,
  new_pcs,
  new_positions,
  aplus_revenue,
  aplus_kg,
  bedding_revenue
)
select
  sale_date,
  store,
  product_group,
  product,
  check_id,
  sum(revenue),
  sum(kg),
  sum(pcs),
  count(*),
  coalesce(sum(revenue) filter (where category = 'second'), 0),
  coalesce(sum(kg) filter (where category = 'second'), 0),
  coalesce(sum(pcs) filter (where category = 'second'), 0),
  count(*) filter (where category = 'second'),
  coalesce(sum(revenue) filter (where category = 'new'), 0),
  coalesce(sum(pcs) filter (where category = 'new'), 0),
  count(*) filter (where category = 'new'),
  coalesce(sum(revenue) filter (where is_aplus), 0),
  coalesce(sum(kg) filter (where is_aplus), 0),
  coalesce(sum(revenue) filter (where is_kpb), 0)
from tmp_ecostok2_line_classified
group by sale_date, store, product_group, product, check_id;

commit;
SQL

log "refresh SQL committed"

VERIFY_OUTPUT="$("${PSQL[@]}" -v ON_ERROR_STOP=1 -v start_date="$START_DATE" -v end_date="$END_DATE" -At <<'SQL'
with raw as (
  select
    sale_date,
    sum(revenue) as raw_revenue,
    count(*) as raw_positions,
    count(distinct coalesce(nullif(split_part(coalesce(recorder_id, ''), '_', 1), ''), id::text)) as raw_checks
  from public.ecostok2_sales_analytics
  where sale_date between date :'start_date' and date :'end_date'
  group by sale_date
),
cmp as (
  select
    r.sale_date,
    r.raw_revenue,
    coalesce(d.total_revenue, 0) as dm_revenue,
    coalesce(d.total_revenue, 0) - r.raw_revenue as diff_revenue,
    r.raw_positions,
    coalesce(d.total_positions, 0) as dm_positions,
    coalesce(d.total_positions, 0) - r.raw_positions as diff_positions,
    r.raw_checks,
    coalesce(d.total_checks_day, 0) as dm_checks,
    coalesce(d.total_checks_day, 0) - r.raw_checks as diff_checks
  from raw r
  left join public.ecostok2_dm_sales_day d on d.sale_date = r.sale_date
)
select
  sale_date || '|raw_revenue=' || raw_revenue ||
  '|dm_revenue=' || dm_revenue ||
  '|diff_revenue=' || diff_revenue ||
  '|raw_positions=' || raw_positions ||
  '|dm_positions=' || dm_positions ||
  '|diff_positions=' || diff_positions ||
  '|raw_checks=' || raw_checks ||
  '|dm_checks=' || dm_checks ||
  '|diff_checks=' || diff_checks
from cmp
where diff_revenue <> 0
   or diff_positions <> 0
   or diff_checks <> 0
order by sale_date;
SQL
)"

if [[ -n "$VERIFY_OUTPUT" ]]; then
  log "ERROR: raw vs dm verification failed"
  printf '%s\n' "$VERIFY_OUTPUT"
  exit 1
fi

log "raw vs dm verification passed"

"${PSQL[@]}" -v ON_ERROR_STOP=1 -v start_date="$START_DATE" -v end_date="$END_DATE" <<'SQL'
select
  r.sale_date,
  r.raw_revenue,
  d.total_revenue as dm_revenue,
  d.total_revenue - r.raw_revenue as diff_revenue,
  r.raw_positions,
  d.total_positions as dm_positions,
  d.total_positions - r.raw_positions as diff_positions,
  r.raw_checks,
  d.total_checks_day as dm_checks,
  d.total_checks_day - r.raw_checks as diff_checks
from (
  select
    sale_date,
    sum(revenue) as raw_revenue,
    count(*) as raw_positions,
    count(distinct coalesce(nullif(split_part(coalesce(recorder_id, ''), '_', 1), ''), id::text)) as raw_checks
  from public.ecostok2_sales_analytics
  where sale_date between date :'start_date' and date :'end_date'
  group by sale_date
) r
join public.ecostok2_dm_sales_day d on d.sale_date = r.sale_date
order by r.sale_date;
SQL

INVENTORY_SNAPSHOT="$("${PSQL[@]}" -At -v start_date="$START_DATE" -v end_date="$END_DATE" <<'SQL'
select max(snapshot_date)::date
from public.ecostok2_inventory_analytics
where snapshot_date between date :'start_date' and date :'end_date';
SQL
)"

if [[ -z "$INVENTORY_SNAPSHOT" ]]; then
  log "ERROR: no inventory snapshot found in range $START_DATE .. $END_DATE"
  exit 1
fi

log "inventory current refresh snapshot: $INVENTORY_SNAPSHOT"

"${PSQL[@]}" -v ON_ERROR_STOP=1 -v inventory_snapshot="$INVENTORY_SNAPSHOT" <<'SQL'
begin;

delete from public.ecostok2_dm_inventory_current;

insert into public.ecostok2_dm_inventory_current (
  snapshot_date,
  store,
  product,
  product_group,
  unit,
  quantity
)
select
  snapshot_date,
  store,
  product,
  max(product_group) as product_group,
  unit,
  sum(quantity) as quantity
from public.ecostok2_inventory_analytics
where snapshot_date = date :'inventory_snapshot'
group by snapshot_date, store, product, unit;

commit;
SQL

INVENTORY_VERIFY_OUTPUT="$("${PSQL[@]}" -v ON_ERROR_STOP=1 -v inventory_snapshot="$INVENTORY_SNAPSHOT" -At <<'SQL'
with raw as (
  select
    snapshot_date,
    count(*) as raw_rows,
    sum(quantity) as raw_quantity
  from public.ecostok2_inventory_analytics
  where snapshot_date = date :'inventory_snapshot'
  group by snapshot_date
),
dm as (
  select
    snapshot_date,
    count(*) as dm_rows,
    sum(quantity) as dm_quantity
  from public.ecostok2_dm_inventory_current
  group by snapshot_date
),
cmp as (
  select
    r.snapshot_date,
    r.raw_rows,
    coalesce(d.dm_rows, 0) as dm_rows,
    coalesce(d.dm_rows, 0) - r.raw_rows as diff_rows,
    r.raw_quantity,
    coalesce(d.dm_quantity, 0) as dm_quantity,
    coalesce(d.dm_quantity, 0) - r.raw_quantity as diff_quantity
  from raw r
  left join dm d on d.snapshot_date = r.snapshot_date
)
select
  snapshot_date || '|raw_rows=' || raw_rows ||
  '|dm_rows=' || dm_rows ||
  '|diff_rows=' || diff_rows ||
  '|raw_quantity=' || raw_quantity ||
  '|dm_quantity=' || dm_quantity ||
  '|diff_quantity=' || diff_quantity
from cmp
where diff_rows <> 0
   or diff_quantity <> 0;
SQL
)"

if [[ -n "$INVENTORY_VERIFY_OUTPUT" ]]; then
  log "ERROR: raw inventory vs dm inventory verification failed"
  printf '%s\n' "$INVENTORY_VERIFY_OUTPUT"
  exit 1
fi

log "raw inventory vs dm inventory verification passed"

"${PSQL[@]}" -v ON_ERROR_STOP=1 -v inventory_snapshot="$INVENTORY_SNAPSHOT" <<'SQL'
select
  r.snapshot_date,
  r.raw_rows,
  d.dm_rows,
  d.dm_rows - r.raw_rows as diff_rows,
  r.raw_quantity,
  d.dm_quantity,
  d.dm_quantity - r.raw_quantity as diff_quantity
from (
  select
    snapshot_date,
    count(*) as raw_rows,
    sum(quantity) as raw_quantity
  from public.ecostok2_inventory_analytics
  where snapshot_date = date :'inventory_snapshot'
  group by snapshot_date
) r
join (
  select
    snapshot_date,
    count(*) as dm_rows,
    sum(quantity) as dm_quantity
  from public.ecostok2_dm_inventory_current
  group by snapshot_date
) d on d.snapshot_date = r.snapshot_date;
SQL

log "ecostok2 DM refresh finished successfully"
