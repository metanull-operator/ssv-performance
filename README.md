# ssv_performance

Clone the ssv_performance repository into your preferred directory.

```
git clone https://github.com/metanull-operator/ssv_performance
cd ssv_performance
```

## ssv-performance-bot & ClickHouse database

The ssv-performance-bot and ClickHouse database run togther in a docker compose application.

### Configure .env

Use your preferred text editor for these commands. Here we will use `vi`.

```
cp .env.sample .env
vi .env
```

Set the following environment variables in `.env`:

- BOT_DISCORD_CHANNEL_ID - A single Discord channel ID on which the bot will respond to commands and post daily messages. You can find the Discord channel ID by right-clicking on the channel name and selecting `Copy Channel ID`
- CLICKHOUSE_PASSWORD - A password for the ClickHouse database. For new databases this will become the default password.

### Store Discord Token

The bot requires a Discord token for API access. Generate the Discord token and put the contents in `credentials/discord-token.txt`.

```
cd credentials
vi discord-token.txt
cd ..
```

Enter the Discord token then save and exit.

### Build ssv-performance-bot Image

```
cd ssv-performance-bot
docker build -t ssv-performance-bot .
cd ..
```

### Start the Applications

To start the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance up --build -d
```

To stop the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance down
```
### Find Docker Network Name

The default Docker network should be named `ssv_performance_ssv-performance-network`, but please confirm the network name. The correct network name is required for other scripts to connect to ClickHouse.

```bash
docker network ls
```

If you have not modified the `docker-compose.yml` file, the correct network name should end with `ssv-performance-network`.

## ssv-performance-collector

The ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores that data in the ClickHouse database. The ClickHouse database must be running to collect performance data.

### Build Image

```
cd scripts/ssv-performance-collector
docker build -t ssv-performance-collector .
vi Dockerfile
```

If you set `CLICKHOUSE_PASSWORD` in `docker/.env` then you must update that value in the `Dockerfile` or include the password in the `CLICKHOUSE_PASSWORD` environment variable when the collector is run.

### Run Collector

Run the collector once per Ethereum network for which you want to collect performance data.

```bash
docker run --rm --network=ssv_performance_ssv-performance-network ssv-performance-collector --network mainnet
docker run --rm --network=ssv_performance_ssv-performance-network ssv-performance-collector --network holesky
docker run --rm --network=ssv_performance_ssv-performance-network ssv-performance-collector --network hoodi
```

Replace `ssv_performance_ssv-performance-network` with the name of the Docker network found earlier, if it isn't the default value shown here. 

Create cronjobs to run the command daily for each network.

## ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. This script assumes that one worksheet in Google Sheets will contain the performance data for a single Ethereum network and performance period (24h/30d).

### Store Google Credentials

Create Google credentials allowing this script to access and edit the target Google Sheet. Store the credentials in `credentials/google-credentials.json`.

```bash
cd credentials
vi google-credentials.json
```

### ClickHouse Password

### Build Image

```
cd scripts/ssv-performance-sheets
docker build -t ssv-performance-sheets .
vi Dockerfile
```

If you set `CLICKHOUSE_PASSWORD` in `docker/.env` then you must update that value in the `Dockerfile` or include the password in the `CLICKHOUSE_PASSWORD` environment variable when the collector is run.

### Run ssv-performance-sheets

The script must be run once for each Ethereum network and performance period. A separate worksheet is required for each Ethereum network and performance period.

```bash
docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 24h' --metric 24h --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 30d' --metric 30d --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 24h' --metric 24h --network holesky

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 30d' --metric 30d --network holesky

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Hoodi 24h' --metric 24h --network hoodi

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv_performance_vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Hoodi 30d' --metric 30d --network hoodi
```

Replace `ssv_performance_ssv-performance-network` with the name of the Docker network found earlier, if it isn't the default value shown here. 

Create cronjobs to run the command daily for each network and performance period. These cronjobs should run after the `ssv-performance-collector` cronjobs.

### Run migration-dyndb-clickhouse

The original DynamoDB database used separate database tables for each Ethereum network. When running `migration-dyndb-clickhouse`, specify the Ethereum network and the DynamoDB source table on the command line.

```bash
docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network mainnet --dynamo-perf-table=SSVPerformanceData

docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network holesky --dynamo-perf-table=SSVPerformanceDataHolesky	
```



## Importing ClickHouse SQL Data

### Export Source Data

Confirm your source ClickHouse container name.

```bash
docker ps
```

If you are exporting data for another to import, the following commands will export the data from the source ClickHouse container, one table at a time.

```bash
docker exec -i ssv_performance_clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM operators FORMAT SQLInsert">  operators.sql
docker exec -i ssv_performance_clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM performance FORMAT SQLInsert" > performance.sql
docker exec -i ssv_performance_clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM performance FORMAT SQLInsert" > subscriptions.sql
docker exec -i ssv_performance_clickhouse-1 clickhouse-client --database=default --query="SELECT * FROM performance FORMAT SQLInsert" > import_state.sql
```

### Import Source Data

Confirm your destination ClickHouse container name.

```bash
docker ps
```

If you are receiving source data to import, the following commands will import the data into the destination ClickHouse container, one table at a time.

```bash
docker exec -i ssv_performance_clickhouse-2 clickhouse-client --database=default < operators.sql
docker exec -i ssv_performance_clickhouse-2 clickhouse-client --database=default < performance.sql
docker exec -i ssv_performance_clickhouse-2 clickhouse-client --database=default < subscriptions.sql
docker exec -i ssv_performance_clickhouse-2 clickhouse-client --database=default < import_state.sql
```

### Optimize the Tables

The ClickHouse database tables use the `ReplacingMergeTree` engine. Running `OPTIMIZE TABLE` on these tables will force a merge of duplicate records.

```bash
docker exec ssv_performance_clickhouse-1 clickhouse-client --database=default --query="OPTIMIZE TABLE operators FINAL"
docker exec ssv_performance_clickhouse-1 clickhouse-client --database=default --query="OPTIMIZE TABLE performance FINAL"
docker exec ssv_performance_clickhouse-1 clickhouse-client --database=default --query="OPTIMIZE TABLE subscriptions FINAL"
docker exec ssv_performance_clickhouse-1 clickhouse-client --database=default --query="OPTIMIZE TABLE import_state FINAL"
```

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



### Build Image

```bash
cd scripts/migration-dyndb-clickhouse
docker build -t ssv-performance-collector .
vi Dockerfile
```

If you set `CLICKHOUSE_PASSWORD` in `docker/.env` then you must update that value in the `Dockerfile` or include the password in the `CLICKHOUSE_PASSWORD` environment variable when the collector is run.