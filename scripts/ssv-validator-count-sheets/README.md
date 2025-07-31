# ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. Performance data for a single Ethereum network and performance period (24h/30d) is copied to a single Google Sheet.

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
docker build -t ssv-validator-count-sheets scripts/ssv-validator-count-sheets/
```

## Identify the ClickHouse Docker Network

The ssv-performance-sheets script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run ssv-performance-sheets

The script must be run once for each Ethereum network. A separate worksheet is required for each Ethereum network.

Replace the following with the correct values:

- `<GOOGLE_SHEETS_DOCUMENT_NAME>` - Name of the Google Sheets document into which data should be stored
- `<GOOGLE_SHEETS_WORKSHEET_NAME>` - Name of the worksheet within the `<GOOGLE_SHEETS_DOCUMENT_NAME>` document into which data should be stored

Set the value of `--network` appropriately for the data you wish to upload.

```bash
docker run --rm -v "./credentials/clickhouse-password.txt:/clickhouse-password.txt:ro" -v "./credentials/google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-validator-count-sheets -d '<GOOGLE_SHEETS_DOCUMENT_NAME>' -w '<GOOGLE_SHEETS_WORKSHEET_NAME>' --network mainnet
```

## Create cronjobs

Create cronjobs to run the command daily for each network. These cronjobs should run after the `ssv-performance-collector` cronjobs to ensure that the Google Sheets have the latest data. Use absolute paths to mount the `clickhouse-password.txt` file and use the Docker network found above.

Here are some example crontab entries to upload validator count data daily for Mainnet and Hoodi. 

```
30 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt:ro" --network ssv-performance_ssv-performance-network ssv-validator-count-sheets -d 'SSV Performance Data' -w 'Mainnet Validator Counts' --network mainnet
35 0 * * * /usr/bin/docker run --rm -v "/opt/ssv-performance/credentials/google-credentials.json:/google-credentials.json:ro" -v "/opt/ssv-performance/credentials/clickhouse-password.txt:/clickhouse-password.txt:ro" --network ssv-performance_ssv-performance-network ssv-validator-count-sheets -d 'SSV Performance Data' -w 'Hoodi Validator Counts' --network hoodi
```