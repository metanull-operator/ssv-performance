# clickhouse-export

The clickhouse-export script allows for simple backups of ClickHouse data and facilitates migration of SSV performance data from one ClickHouse database instance to another.

## Export ClickHouse Data

### Configure Export Directory

By default, exported SQL files will go into a dated subdirectory within the `sql-export/` directory of the repository. Edit the `.env` file and set the `EXPORT_DIR` environment variable value to a different location, if necessary.

### Source ClickHouse Container Name

Confirm your source ClickHouse container name.

```bash
docker ps
```

Replace `ssv-performance-clickhouse-1` in the commands below with the name of your source ClickHouse container.

### Run Exports Commands

The following command will export all SQL tables from the source ClickHouse container. The data from each table will be stored in a file named after the table.

```bash
docker exec -i ssv-performance-clickhouse-1 clickhouse-export.sh
```