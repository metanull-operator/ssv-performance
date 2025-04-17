# ssv_performance

Clone the ssv_performance repository into your preferred directory. Here we will use `/opt` as an example.

```
cd /opt
git clone https://github.com/metanull-operator/ssv_performance
cd ssv_performance
```

## ssv-performance-bot & ClickHouse database

The ssv-performance-bot and ClickHouse database run togther in a docker compose application.

### Configure .env

Use your preferred text editor for these commands. Here we will use `vi`.

```
cd docker
cp .env.sample .env
vi .env
cd ..
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

### Start the Applications

To start the ssv-performance-bot and ClickHouse database:

```
docker compose -f docker/docker-compose.yml -p ssv-performance up --build -d
```

To stop the ssv-performance-bot and ClickHouse database:

```
docker compose -f docker/docker-compose.yml -p ssv-performance down
```
### Find Docker Network Name

Find the Docker network for ClickHouse. You will need this to connect other scripts to ClickHouse.

```bash
docker network ls
```

The network you need will end with `ssv-performance-network`. If you have not changed the default name of the repository directory, the value will likely be `ssv_performance_ssv-performance-network`.

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

## migration-dyndb-clickhouse

This data migration script will transfer performance data from the existing AWS DynamoDB database to the new ClickHouse database. It must be run once for each Ethereum network migrated (mainnet/holesky).

### Store AWS Credentials

```bash
cd credentials/aws
vi credentials
```

Insert values for `aws_access_key_id` and `aws_secret_access_key`, then save and exit the file.

### ClickHouse Password



### Run migration-dyndb-clickhouse

The original DynamoDB database used separate database tables for each Ethereum network. When running `migration-dyndb-clickhouse`, specify the Ethereum network and the DynamoDB source table on the command line.

```bash
docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network mainnet --dynamo-perf-table=SSVPerformanceData

docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws:ro" migration-dyndb-clickhouse --network holesky --dynamo-perf-table=SSVPerformanceDataHolesky	
```