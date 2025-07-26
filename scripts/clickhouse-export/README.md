# clickhouse-import

The clickhouse-import script facilitates migration of SSV performance data from one ClickHouse database instance to another.

High-level overview of migration process:

- Export source SQL data from source ClickHouse instance
- Copy data files to the `sql-import/` directory
- Import source SQL data to destination ClickHouse instance

## Export Source Data

Source SQL data must be exported from ClickHouse using the following procedure. If source SQL data has already been provided, you may skip the Export Source Data section.

### Source ClickHouse Container Name

Confirm your source ClickHouse container name.

```bash
docker ps
```

Replace `ssv-performance-clickhouse-1` in the commands below with the name of your source ClickHouse container.

### Run Exports Commands

The following commands will export performance data from the source ClickHouse container, one table at a time.

```bash
docker exec -it ssv-performance-clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM operators FORMAT SQLInsert" >  operators.sql
docker exec -it ssv-performance-clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM performance FORMAT SQLInsert" > performance.sql
docker exec -it ssv-performance-clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM performance FORMAT SQLInsert" > subscriptions.sql
docker exec -it ssv-performance-clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM validator_counts FORMAT SQLInsert" > validator_counts.sql
docker exec -it ssv-performance-clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM operator_fees FORMAT SQLInsert" > operator_fees.sql
```

## Copy SQL Files to `sql-import/`

Once you have your source SQL data files, use your preferred tools to copy the files to the `sql-import/` directory of the `ssv-performance` repository. The filenames must match the names of the tables into which the data is being imported. For example, data in the `operators.sql` file will be imported into the `operators` table.

You should have files for the following tables:
- operators
- performance
- subscriptions
- validator_counts
- operator_fees

## Import Source Data

### Destination ClickHouse Container Name

Confirm your destination ClickHouse container name.

```bash
docker ps
```

Replace `ssv-performance-clickhouse-1` in the commands below with the name of your destination ClickHouse container.

### Run Import Command

Run the `clickhouse-import.sh` script in the destination ClickHouse container. This script will import all `.sql` files in the `sql-import/` directory into the ClickHouse database.

```bash
docker exec -i ssv-performance-clickhouse-1 clickhouse-import.sh
```