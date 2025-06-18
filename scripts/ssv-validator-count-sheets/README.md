# ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. Performance data for a single Ethereum network and performance period (24h/30d) is copied to a single Google Sheet.

## Store Google Credentials

Create Google credentials allowing this script to access and edit the target Google Sheet. Store the credentials in `credentials/google-credentials.json`.

```bash
cd credentials
cp google-credentials.json.sample google-credentials.json
vi google-credentials.json
```

## Build Image

```
docker build -t ssv-performance-sheets ./scripts/ssv-performance-sheets
```

## Identify the ClickHouse Docker Network

The ssv-performance-sheets script must have access to the same Docker network on which the ClickHouse database is running. Identify the network and replace `ssv-performance_ssv-performance-network` in the commands below with the identified network.

```bash
docker network ls
```

## Run ssv-performance-sheets

The script must be run once for each Ethereum network and performance period. A separate worksheet is required for each Ethereum network and performance period.

```bash
docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 24h' --metric 24h --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Mainnet 30d' --metric 30d --network mainnet

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 24h' --metric 24h --network holesky

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Holesky 30d' --metric 30d --network holesky

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Hoodi 24h' --metric 24h --network hoodi

docker run --rm -v ".\credentials\google-credentials.json:/google-credentials.json:ro" --network ssv-performance_ssv-performance-network ssv-performance-sheets -d 'ssv_performance_data' -w 'Hoodi 30d' --metric 30d --network hoodi
```

Create cronjobs to run the command daily for each network and performance period. These cronjobs should run after the `ssv-performance-collector` cronjobs to ensure that the Google Sheets have the latest data.