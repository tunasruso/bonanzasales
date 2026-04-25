# Ecostok2 - Архитектура и пайплайн данных

## 1. Общая схема

```text
1C PostgreSQL
  -> Local Sync (Python, 172.16.0.45)
  -> Supabase self-hosted на сервере 187.77.156.98
  -> ecostok2_* raw tables
  -> ecostok2_dm_* витрины
  -> Fast RPC через PostgREST/Kong
  -> Frontend https://ecostok2.lidertex.cloud
```

## 2. Серверы и адреса

| Узел | Адрес | Назначение |
|---|---|---|
| Hostinger VPS | `187.77.156.98` | Supabase ecostok, Nginx, Traefik, frontend ecostok2 |
| Local sync PC | `172.16.0.45` | Python sync из 1С в Supabase raw tables |
| Supabase Kong на сервере | `http://127.0.0.1:8100` | REST/RPC API для ecostok2 |
| ecostok2 frontend | `https://ecostok2.lidertex.cloud` | Dashboard |

## 3. Пути

| Путь | Назначение |
|---|---|
| `/root/bonanzasales2-ecostok2/` | Код ecostok2, sync scripts, refresh script |
| `/root/bonanzasales2-ecostok2/sales-dashboard/` | React/Vite frontend |
| `/opt/ecostok2-sales-dashboard/dist/` | Published frontend dist |
| `/root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh` | Refresh sales/inventory DM витрин |
| `/root/bonanzasales2-ecostok2/logs/dm_refresh.log` | Лог refresh cron |
| `/etc/nginx/sites-available/ecostok2.lidertex.cloud` | Nginx config |
| `/root/traefik/config/ecostok2.yml` | Traefik file provider config |

## 4. Источники данных 1С

| Данные | 1С таблица |
|---|---|
| Продажи | `_AccumRg53715` |
| Остатки | `_AccumRg52568` |
| Посетители | `_AccumRg53554` |

## 5. Local Sync

Local sync запускается на Linux PC `172.16.0.45` и пишет только raw таблицы Supabase.

Активные серверные копии скриптов:

| Скрипт | Назначение |
|---|---|
| `/root/bonanzasales2-ecostok2/sync_to_supabase.py` | Sales sync в `ecostok2_sales_analytics` |
| `/root/bonanzasales2-ecostok2/custom_inventory_sync.py` | Inventory sync в `ecostok2_inventory_analytics` |
| `/root/bonanzasales2-ecostok2/sync_visitors.py` | Visitors sync в `ecostok2_visitors_analytics` |
| `/root/bonanzasales2-ecostok2/check_supabase_sales.py` | Быстрая проверка sales в Supabase |

Env:

```bash
export SUPABASE_URL="https://ecostok2.lidertex.cloud"
export SUPABASE_SERVICE_KEY="<service-role-key>"
export POSTGRES_HOST="<1c-postgres-host>"
export POSTGRES_PORT="<1c-postgres-port>"
export POSTGRES_DB="<1c-db>"
export POSTGRES_USER="<1c-user>"
export POSTGRES_PASSWORD="<1c-password>"
```

Логика:

- Sales: UPSERT по `recorder_id`, sliding window последние 3 дня.
- Inventory: DELETE по `snapshot_date`, затем INSERT snapshot.
- Inventory дедупликация: `group by (store, product, snapshot_date, unit)`, `sum(quantity)`.
- Visitors: UPSERT по `(visit_date, store)`.

## 6. Raw таблицы Supabase

| Таблица | Назначение |
|---|---|
| `public.ecostok2_sales_analytics` | Сырые продажи |
| `public.ecostok2_inventory_analytics` | Сырые остатки snapshot |
| `public.ecostok2_visitors_analytics` | Трафик по дням и магазинам |
| `public.ecostok2_product_weights` | Правила веса/classification second/new |
| `public.ecostok2_product_classifications` | Дополнительные классификации |
| `public.ecostok2_app_users` | Логин frontend |

## 7. DM витрины

| Витрина | Назначение |
|---|---|
| `public.ecostok2_dm_product_weights_resolved` | Разрешенный mapping товар -> вес/category |
| `public.ecostok2_dm_sales_day` | Sales KPI по дням |
| `public.ecostok2_dm_sales_day_shop` | Sales KPI по дням и магазинам |
| `public.ecostok2_dm_sales_check_shop_category` | Check/category pre-aggregation |
| `public.ecostok2_dm_sales_check_shop_product` | Check/product pre-aggregation |
| `public.ecostok2_dm_inventory_current` | Текущий inventory snapshot |

Важно:

- Inventory хранится как current snapshot.
- Sales хранятся как агрегаты по дням и check-level витрины.
- Frontend не делает JOIN по raw sales.

## 8. Fast RPC

| RPC | Назначение |
|---|---|
| `ecostok2_get_kpis_aggregated_fast` | KPI cards |
| `ecostok2_get_shop_kpis_aggregated_fast` | Таблица магазинов |
| `ecostok2_get_pivot_aggregated_fast` | Сводная таблица и графики |
| `ecostok2_get_daily_dynamics_fast` | Дневная динамика |

Ожидаемое время ответа: около `1 ms` для диапазона 2-3 месяца.

## 9. Frontend

URL:

```text
https://ecostok2.lidertex.cloud
```

Использует:

- Sales -> fast RPC.
- Inventory -> `ecostok2_dm_inventory_current`.
- Visitors -> прямой REST select из `ecostok2_visitors_analytics`.
- Login -> `ecostok2_app_users`.

## 10. Cron

На сервере `187.77.156.98`:

```cron
*/30 * * * * flock -n /tmp/ecostok2_dm_refresh.lock /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh >> /root/bonanzasales2-ecostok2/logs/dm_refresh.log 2>&1
```

Назначение:

- обновляет DM витрины;
- берет диапазон `max(sale_date)-2 .. max(sale_date)`;
- не тянет данные из 1С;
- не рестартует Docker/Supabase.

## 11. Принципы системы

- Никаких JOIN raw в frontend.
- Никаких тяжелых запросов в UI.
- Все отчеты по продажам через DM/Fast RPC.
- Все расчеты в Postgres.
- Local sync только пишет raw.
- Серверный cron только пересчитывает DM.

## 12. Ограничения

- Visitors за текущий день могут отсутствовать.
- Inventory в dashboard - current snapshot, не история.
- CDC нет, используется sliding window 3 дня.

## 13. Точки контроля

```text
raw vs dm diff = 0
dm vs fast_rpc diff = 0
latency < 5 ms
```

