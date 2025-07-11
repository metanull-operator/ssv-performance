# ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. Performance data for a single Ethereum network and a single performance period (24h/30d) is copied to a single Google Sheet.

## Store Google Credentials

Create Google credentials allowing this script to access and edit the target Google Sheet. Store the credentials in `credentials/google-credentials.json`.

See the [Google Credentials instructions](../../docs/google-credentials.md) for details of configuring Google Credentials.

```bash
cd credentials
cp google-credentials.json.sample google-credentials.json
vi google-credentials.json
```

## Build Image

```bash
docker build -t ssv-performance-sheets:latest ./scripts/ssv-performance-sheets/
```

## Identify the ClickHouse Docker Network

The ssv-performance-sheets script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run ssv-performance-sheets

The script must be run once for each Ethereum network and performance period. A separate worksheet is required for each Ethereum network and performance period.

```bash
docker run --rm -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "./credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets:latest -d 'VOC Performance Data' -w 'Mainnet 30d' --metric 30d --network mainnet
```

Create cronjobs to run the command daily for each network and performance period. These cronjobs should run after the `ssv-performance-collector` cronjobs to ensure that the Google Sheets have the latest data.