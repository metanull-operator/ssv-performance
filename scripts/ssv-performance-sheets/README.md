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