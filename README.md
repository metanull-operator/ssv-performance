# ssv_performance

## ssv-performance-bot

*Configure*
- .env
- ClickHouse username & password
- Discord Token

docker-compose -f docker/docker-compose.yml up --build

## ssv-performance-collector

*Configure*
- ClickHouse username & password

docker run --rm --network=vo-performance-network ssv-performance-collector --utc --network mainnet
docker run --rm --network=vo-performance-network ssv-performance-collector --utc --network holesky

## ssv-performance-sheets

*Configure*
- Google credentials
- ClickHouse username & password

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json" --network vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 24h' --metric 24h --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json" --network vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 30d' --metric 30d --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json" --network vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 24h' --metric 24h --network holesky

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json" --network vo-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 30d' --metric 30d --network holesky

## migration-dyndb-clickhouse

*Configure*
- AWS credentials
- ClickHouse username & password

docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws" migration-dyndb-clickhouse --network mainnet --dynamo-perf-table=SSVPerformanceData

docker run --rm --network=vo-performance-network -v ".\credentials\aws:/root/.aws" migration-dyndb-clickhouse --network holesky --dynamo-perf-table=SSVPerformanceDataHolesky