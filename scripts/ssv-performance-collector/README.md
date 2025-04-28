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