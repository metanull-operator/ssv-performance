# ssv-performance-collector

The ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores that data in the ClickHouse database. The ClickHouse database must be running to collect performance data.

## Docker

### Build Image

```
docker build -t ssv-performance-collector ./scripts/ssv-performance-collector
```

### Identify the ssv-performance Docker Network

The ssv-performance-collector script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

### Run Collector

Run the collector once for each Ethereum network for which you want to collect performance data.

```bash
docker run --rm -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network mainnet
```

### Verified Operator Staleness Sweep

At the end of each collector run the collector will remove Verified Operator status from all operators that have not appeared in SSV API results for 14 days. Without this, an operator who removes their public record from the API would keep `is_vo=1` forever, because no further updates ever arrive for them.

When the sweep fires, affected operators have:

- `is_vo` set to `0`.
- `vo_demoted_at` stamped with the current UTC date.

Both fields are reset automatically on any subsequent run in which the API again returns the operator.

**Safety** The sweep is deliberately conservative and is skipped entirely if today's fetch looks degraded, so an API outage cannot silently strip verified status from every operator:

- `VO_SWEEP_MIN_OPERATORS` (default `100`) - Absolute floor on today's operator count. If fewer operators are returned, no demotions occur.
- `VO_SWEEP_MIN_COVERAGE` (default `0.9`) - Today's fetch must cover at least this fraction of operators the DB has seen in the last `VO_STALENESS_DAYS` days. If coverage is lower, no demotions occur.

**Environment variables.**

| Variable | Default | Description |
|---|---|---|
| `VO_STALENESS_DAYS` | `14` | Demote `is_vo=1` operators whose DB row has not been refreshed within this many days. |
| `VO_SWEEP_MIN_OPERATORS` | `100` | Skip the sweep unless today's fetch returned at least this many operators. |
| `VO_SWEEP_MIN_COVERAGE` | `0.9` | Skip the sweep unless today's fetch covers at least this fraction of recently-seen operators. |

Equivalent command-line flags are `--vo-staleness-days`, `--vo-sweep-min-operators`, and `--vo-sweep-min-coverage`.

### Optional Consensus API Validator Status

For the most accurate active validator count, a consensus client connection is required. The validator count from the SSV API does not correctly account for removed validators and may not provide the most accurate "active" statuses. Optionally provide a consensus client URL to the script to have validator statuses pulled directly from the consensus layer.

Include the environment variable `BEACON_API_URL` or the command-line parameter `--beacon-api-url` to specify the URL to a consensus client API. If present, the script will contact the API for the latest validator status information.

On the command line:
```
--beacon-api-url=http://<CONSENSUS_ADDR>:<CONSENSUS_PORT>/
```

If a beacon API URL is not specified, the status from the SSV API will be used instead.

## Standalone

### Install Required Python Packages

```bash
pip3 install clickhouse_connect requests
```

### Run ssv-performance-collector

```bash
python3 scripts/ssv-performance-collector/ssv-performance-collector.py --network mainnet -p credentials/clickhouse-password.txt --beacon-api-url=http://<CONSENSUS_ADDR>:<CONSENSUS_PORT>/
```

The script assumes that the ClickHouse database is accessible at port 8123 on the localhost. The environment variables `CLICKHOUSE_PORT` and `CLICKHOUSE_HOST` may be used to specify a different port or host at which to access the ClickHouse database.

## Create cronjobs

Create separate cronjobs to run the command daily for each network. Use absolute paths to mount the `clickhouse-password.txt` file.

Here are some example crontab entries to run the collector daily for Mainnet and Hoodi. 

## Docker

Use the Docker network found above.

```
0 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network mainnet
5 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" --network ssv-performance_ssv-performance-network ssv-performance-collector --network hoodi
```

## Standalone

```
0 0 * * * /usr/bin/python3 /opt/ssv-performance/scripts/ssv-performance-collector/ssv-performance-collector.py --network mainnet -p /opt/ssv-performance/credentials/clickhouse-password.txt --beacon-api-url=http://localhost:3500/
5 0 * * * /usr/bin/python3 /opt/ssv-performance/scripts/ssv-performance-collector/ssv-performance-collector.py --network hoodi -p /opt/ssv-performance/credentials/clickhouse-password.txt --beacon-api-url=http://localhost:3500/
```
