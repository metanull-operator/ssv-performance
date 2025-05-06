# migration-dyndb-clickhouse (Deprecated)

This data migration script will transfer performance data from an AWS DynamoDB database to a ClickHouse database. It must be run once for each Ethereum network migrated (mainnet/holesky/hoodi). 

Because performance data has already been migrated from the production AWS DynamoDB database to a ClickHouse database instance, this script is deprecated.

## Store AWS Credentials

Generate AWS DynamoDB credentials.

```bash
cd credentials
cp credentials.sample credentials
vi credentials
cd ..
```

Insert values for `aws_access_key_id` and `aws_secret_access_key`, then save and exit the file.

## ClickHouse Password

TBD

## Build Image

```bash
docker build -t migration-dyndb-clickhouse ./scripts/migration-dyndb-clickhouse
```

## Identify the ClickHouse Docker Network

The migration-dyndb-clickhouse script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run migration-dyndb-clickhouse

The original DynamoDB database used separate database tables for each Ethereum network. When running `migration-dyndb-clickhouse`, specify the Ethereum network and the DynamoDB source table on the command line.

```bash
docker run --rm --network=ssv-performance_ssv-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network mainnet --dynamo-perf-table=SSVPerformanceData

docker run --rm --network=ssv-performance_ssv-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network holesky --dynamo-perf-table=SSVPerformanceDataHolesky	
```
