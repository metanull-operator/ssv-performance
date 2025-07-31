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
docker build -t ssv-performance-sheets ./scripts/ssv-performance-sheets/
```

## Identify the ssv-performance Docker Network

The ssv-performance-sheets script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run ssv-performance-sheets

The script must be run once for each Ethereum network and performance period. A separate worksheet is required for each Ethereum network and performance period.

Replace the following with the correct values:
- `<GOOGLE_SHEETS_DOCUMENT_NAME>` - Name of the Google Sheets document into which data should be stored
- `<GOOGLE_SHEETS_WORKSHEET_NAME>` - Name of the worksheet within the `<GOOGLE_SHEETS_DOCUMENT_NAME>` document into which data should be stored

Set the values of `--metric` and `--network` appropriately for the data you wish to upload.

```bash
docker run --rm -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "./credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d '<GOOGLE_SHEETS_DOCUMENT_NAME>' -w '<GOOGLE_SHEETS_WORKSHEET_NAME>' --metric 30d --network mainnet
```

## Create cronjobs

Create cronjobs to run the command daily for each network and performance period. These cronjobs should run after the `ssv-performance-collector` cronjobs to ensure that the Google Sheets have the latest data. Use absolute paths to mount the `clickhouse-password.txt` file and use the Docker network found above.

Here are some example crontab entries to run the collector daily for Mainnet and Hoodi. 

```
10 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'SSV Performance Data' -w 'Mainnet 30d' --metric 30d --network mainnet
15 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'SSV Performance Data' -w 'Mainnet 24h' --metric 24h --network mainnet
20 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'SSV Performance Data' -w 'Hoodi 30d' --metric 30d --network hoodi
25 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt" -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'SSV Performance Data' -w 'Hoodi 24h' --metric 24h --network hoodi
```