# Migrations

`clickhouse/init.sql` is applied only on a fresh database initialization. When schema changes land on an existing deployment, the changes listed below must be applied manually. Each migration is idempotent and safe to re-run.

Each entry includes:
- The change it introduces.
- Why it is needed.
- The SQL to apply it against a running ClickHouse container.

## Add `vo_demoted_at` to `operators`

**Why**

Operators who remove their public record from the SSV API stop receiving updates, so `is_vo=1` could persist indefinitely on the stored row. The collector now performs a staleness sweep that demotes such operators (`is_vo` set to `0`) and stamps `vo_demoted_at` with the date of demotion. The bot's daily alert post uses `vo_demoted_at` to render a "Recently Removed Operators" section.

Without this column, the collector will fail on its next run.

**SQL**

```sql
ALTER TABLE default.operators
    ADD COLUMN IF NOT EXISTS vo_demoted_at Nullable(Date) AFTER address;
```

**Apply Against a Running Container**

```bash
docker compose exec clickhouse bash -c 'clickhouse-client \
  --user "${CLICKHOUSE_USER:-ssv_performance}" \
  --password "$(cat /clickhouse-password.txt)" \
  -q "ALTER TABLE default.operators ADD COLUMN IF NOT EXISTS vo_demoted_at Nullable(Date) AFTER address"'
```

**Verify**

```bash
docker compose exec clickhouse bash -c 'clickhouse-client \
  --user "${CLICKHOUSE_USER:-ssv_performance}" \
  --password "$(cat /clickhouse-password.txt)" \
  -q "DESCRIBE TABLE default.operators"'
```

`vo_demoted_at` should appear as `Nullable(Date)`.
