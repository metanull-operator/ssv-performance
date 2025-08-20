# ssv-performance-collector

The ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores that data in the ClickHouse database. The ClickHouse database must be running to collect performance data.

## Build Image

```
docker build -t ssv-performance-collector ./scripts/ssv-performance-collector
```

## Identify the ssv-performance Docker Network

The ssv-performance-collector script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run Collector

Run the collector once for each Ethereum network for which you want to collect performance data.

```bash
docker run --rm -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network mainnet
```

## Create cronjobs

Create separate cronjobs to run the command daily for each network. Use absolute paths to mount the `clickhouse-password.txt` file and use the Docker network found above.

Here are some example crontab entries to run the collector daily for Mainnet and Hoodi. 

```
0 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network mainnet
5 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network hoodi
```

## Optional Consensus API Validator Status

Validator statuses are required in order to get an accurate count of the number of active validators. The SSV API from which operator data is drawn should provide both validator public key and status information for every validator associated with an operator. You may optionally provide a consensus client URL to the script to have validator statuses pulled directly from the consensus layer.

Include the environment variable `BEACON_API_URL` to specify the URL to a consensus client API. If present, the script will contact the API for the latest validator status information.

On the command line:
```
-e "BEACON_API_URL=http://<CONSENSUS_ADDR>:<CONSENSUS_PORT>/"
```

If `BEACON_API_URL` is not specified, the status from the SSV API will be used instead.