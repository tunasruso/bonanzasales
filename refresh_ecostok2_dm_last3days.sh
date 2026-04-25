#!/usr/bin/env bash
set -euo pipefail

DATE_TO="${1:-$(date -u +%F)}"
DATE_FROM="$(date -u -d "${DATE_TO} -2 days" +%F)"

if [[ -n "${SUPABASE_DB_URL:-}" ]]; then
  PSQL=(psql "$SUPABASE_DB_URL")
else
  : "${PGHOST:?PGHOST is required if SUPABASE_DB_URL is not set}"
  : "${PGPORT:?PGPORT is required if SUPABASE_DB_URL is not set}"
  : "${PGDATABASE:?PGDATABASE is required if SUPABASE_DB_URL is not set}"
  : "${PGUSER:?PGUSER is required if SUPABASE_DB_URL is not set}"
  : "${PGPASSWORD:?PGPASSWORD is required if SUPABASE_DB_URL is not set}"
  PSQL=(psql)
fi

echo "Refreshing ecostok2 DM objects for window ${DATE_FROM}..${DATE_TO}"

"${PSQL[@]}" \
  -v ON_ERROR_STOP=1 \
  -v date_from="${DATE_FROM}" \
  -v date_to="${DATE_TO}" <<'SQL'
DO $$
DECLARE
  obj text;
  objects text[] := ARRAY[
    'public.ecostok2_dm_sales_day',
    'public.ecostok2_dm_sales_day_shop',
    'public.ecostok2_dm_sales_check_shop_category',
    'public.ecostok2_dm_sales_check_shop_product',
    'public.ecostok2_dm_inventory_current',
    'public.ecostok2_dm_product_weights_resolved'
  ];
BEGIN
  FOREACH obj IN ARRAY objects LOOP
    IF EXISTS (
      SELECT 1
      FROM pg_matviews
      WHERE schemaname = split_part(obj, '.', 1)
        AND matviewname = split_part(obj, '.', 2)
    ) THEN
      EXECUTE format('REFRESH MATERIALIZED VIEW %s', obj);
      RAISE NOTICE 'Refreshed materialized view: %', obj;
    ELSE
      RAISE NOTICE 'Skipped non-materialized object: %', obj;
    END IF;
  END LOOP;
END $$;
SQL
