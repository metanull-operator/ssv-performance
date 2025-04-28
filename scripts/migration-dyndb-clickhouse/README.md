## migration-dyndb-clickhouse

This data migration script will transfer performance data from the existing AWS DynamoDB database to the new ClickHouse database. It must be run once for each Ethereum network migrated (mainnet/holesky). 

This is deprecated, because the data has already been migrated in production to a new ClickHouse database. See the instruction above for importing.

### Store AWS Credentials

```bash
cd credentials/aws
vi credentials
```

Insert values for `aws_access_key_id` and `aws_secret_access_key`, then save and exit the file.

### ClickHouse Password

TBD

### Build Image

```bash
cd scripts/migration-dyndb-clickhouse
docker build -t ssv-performance-collector .
vi Dockerfile
```

If you set `CLICKHOUSE_PASSWORD` in `docker/.env` then you must update that value in the `Dockerfile` or include the password in the `CLICKHOUSE_PASSWORD` environment variable when the collector is run.

### Run migration-dyndb-clickhouse

The original DynamoDB database used separate database tables for each Ethereum network. When running `migration-dyndb-clickhouse`, specify the Ethereum network and the DynamoDB source table on the command line.

```bash
docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network mainnet --dynamo-perf-table=SSVPerformanceData

docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network holesky --dynamo-perf-table=SSVPerformanceDataHolesky	
```
