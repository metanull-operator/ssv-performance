# clickhouse-export

The clickhouse-export script allows for simple backups of ClickHouse data and facilitates migration of SSV performance data from one ClickHouse database instance to another. Backups will be stored in a subdirectory of `EXPORT_DIR` named for the date and time the export is taken.

An option to have the `clickhouse-export.sh` delete older backups is available by setting the `DAYS_TO_KEEP` environment variable.

## Export ClickHouse Data

### Configure .env

By default, exported SQL files will go into a dated subdirectory within the `sql-export/` directory of the repository. Edit the `.env` file and set the `EXPORT_DIR` environment variable value to a different location, if necessary.

To turn on deletion of older backups, set the `DAYS_TO_KEEP` environment variable to a positive non-zero value representing the number of days of backups to be retained. Backups older than the current date minus `DAYS_TO_KEEP` will be deleted after the current export has been completed. The default value of zero will result in all backups being retained.

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

## Create cronjob

An example cronjob that will export all database tables nightly and delete older backups, if configured.

```
35 0 * * * /usr/bin/docker exec -i ssv-performance-clickhouse-1 clickhouse-export.sh
```