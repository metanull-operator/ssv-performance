# ssv-performance-collector

The ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores that data in the ClickHouse database. The ClickHouse database must be running to collect performance data.

## Build Image

```
docker build -t ssv-performance-collector ./scripts/ssv-performance-collector
docker build -t ssv-performance-sheets:latest ./scripts/ssv-performance-sheets/
```

## Identify the ClickHouse Docker Network

The ssv-performance-collector script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run Collector

Run the collector once for each Ethereum network for which you want to collect performance data.

```bash
docker run --rm --network=ssv-performance_ssv-performance-network ssv-performance-collector --network mainnet
docker run --rm --network=ssv_performance_ssv-performance-network ssv-performance-collector --network holesky
docker run --rm --network=ssv_performance_ssv-performance-network ssv-performance-collector --network hoodi

docker run --rm -it -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector:latest --network mainnet
```

Create cronjobs to run the command daily for each network.