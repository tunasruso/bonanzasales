# Ecostok2 Sync

## Роли

| Узел | Роль |
|---|---|
| Local PC `172.16.0.45` | Тянет данные из 1С PostgreSQL и пишет raw `ecostok2_*` |
| Server `187.77.156.98` | Пересчитывает DM витрины и раздает dashboard |

Серверный cron не ходит в 1С. Он только пересчитывает DM из уже загруженных raw таблиц.

## 1С источники

| Данные | Таблица |
|---|---|
| Sales | `_AccumRg53715` |
| Inventory | `_AccumRg52568` |
| Visitors | `_AccumRg53554` |

## Supabase raw targets

| Target | Writer |
|---|---|
| `ecostok2_sales_analytics` | `sync_to_supabase.py` |
| `ecostok2_inventory_analytics` | `custom_inventory_sync.py` |
| `ecostok2_visitors_analytics` | `sync_visitors.py` |

## Env для local sync

```bash
export SUPABASE_URL="https://ecostok2.lidertex.cloud"
export SUPABASE_SERVICE_KEY="<service-role-key>"
export POSTGRES_HOST="<1c-postgres-host>"
export POSTGRES_PORT="<1c-postgres-port>"
export POSTGRES_DB="<1c-db>"
export POSTGRES_USER="<1c-user>"
export POSTGRES_PASSWORD="<1c-password>"
```

Не коммитить ключи и пароли в repo.

## Sales sync

Скрипт:

```bash
cd /root/bonanzasales2-ecostok2
python3 sync_to_supabase.py
```

Target:

```text
public.ecostok2_sales_analytics
```

Правила:

- UPSERT по `recorder_id`.
- Исключается `Document1009` (`_RecorderTRef = 000003f1`).
- Для регулярного локального запуска использовать окно последних 3 дней.

## Inventory sync

Скрипт:

```bash
cd /root/bonanzasales2-ecostok2
python3 custom_inventory_sync.py 2026-04-25
```

Target:

```text
public.ecostok2_inventory_analytics
```

Правила:

- DELETE по `snapshot_date`.
- INSERT snapshot.
- Дедупликация: `group by (store, product, snapshot_date, unit)`, `sum(quantity)`.
- `ecostok2_inventory_analytics.id` имеет sequence/default:
  `public.ecostok2_inventory_analytics_id_seq`.

## Visitors sync

Скрипт:

```bash
cd /root/bonanzasales2-ecostok2
python3 sync_visitors.py
```

Target:

```text
public.ecostok2_visitors_analytics
```

Правила:

- UPSERT по `(visit_date, store)`.
- Frontend читает visitors напрямую, без DM.
- Visitors за текущий день могут отсутствовать.

## Server DM refresh

Скрипт:

```bash
/root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

Ручной запуск:

```bash
bash /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

Cron:

```cron
*/30 * * * * flock -n /tmp/ecostok2_dm_refresh.lock /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh >> /root/bonanzasales2-ecostok2/logs/dm_refresh.log 2>&1
```

Refresh:

- `ecostok2_dm_product_weights_resolved`
- `ecostok2_dm_sales_day`
- `ecostok2_dm_sales_day_shop`
- `ecostok2_dm_sales_check_shop_category`
- `ecostok2_dm_sales_check_shop_product`
- `ecostok2_dm_inventory_current`

## Проверка после local sync

1. Запустить server refresh:

```bash
bash /root/bonanzasales2-ecostok2/refresh_ecostok2_dm_last3days.sh
```

2. Проверить лог:

```bash
tail -80 /root/bonanzasales2-ecostok2/logs/dm_refresh.log
```

3. Проверить dashboard:

```text
https://ecostok2.lidertex.cloud
```

## Контрольные условия

```text
raw sales revenue = dm sales revenue = fast RPC revenue
raw sales positions = dm sales positions = fast RPC positions
raw sales checks = dm sales checks = fast RPC checks
raw inventory latest snapshot = dm_inventory_current
```

