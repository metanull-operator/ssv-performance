from clickhouse_connect import create_client
from datetime import datetime, timezone, timedelta
import requests
import argparse
import time
import os
import logging

OPERATOR_PERFORMANCE_DAYS = int(os.environ.get('OPERATOR_PERFORMANCE_DAYS', 7)) # Number of 24h data points for /operator command
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 20)) # Total requests to API per minute
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE

IMPORT_SOURCE = os.environ.get("IMPORT_SOURCE", 'api.ssv.network')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clickhouse_client():
    return create_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database=os.environ.get("CLICKHOUSE_DB", "default")
    )


def fetch_and_filter_data(base_url, page_size):
    page = 1
    operators = {}

    while True:
        url = f"{base_url}&page={page}&perPage={page_size}"
        logging.info(f"Getting page {page} of performance data from API")

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"API request failed: {e}")
            break

        data = response.json()

        if not data["operators"]:
            break

        for op in data["operators"]:
            try:
                if int(op["validators_count"]) > 0:
                    op["performance"]['24h'] = float(op["performance"]['24h']) / 100
                    op["performance"]['30d'] = float(op["performance"]['30d']) / 100
                operators[int(op["id"])] = op
            except Exception as e:
                logging.error(f"Error processing operator {op['id']}: {e}")
                continue

        page += 1
        time.sleep(REQUEST_DELAY)

    return operators

def insert_clickhouse_performance_data(client, network, clickhouse_table_operators, clickhouse_table_performance, operators, target_date, source):
    performance_rows = []
    operator_rows = []

    now = datetime.now(timezone.utc)

    for operator_id, operator in operators.items():
        performance_24h = operator["performance"]['24h']
        performance_30d = operator["performance"]['30d']

        is_vo = 1 if operator.get("type", "") == "verified_operator" else 0
        is_private = 1 if operator.get("is_private", False) else 0

        operator_rows.append((
            network,
            operator_id,
            operator.get("name", ""),
            is_vo,
            is_private,
            operator.get("validators_count", 0),
            operator.get("owner_address", ""),
            now
        ))

        performance_rows.append((
            network, 
            operator_id,
            '24h',
            target_date,
            performance_24h,
            source,
            now
        ))

        performance_rows.append((
            network, 
            operator_id,
            '30d',
            target_date,
            performance_30d,
            source,
            now
        ))

    logging.info(f"Inserting/updating {len(operator_rows)} operator records and {len(performance_rows)} performance records into database.")

    client.insert(clickhouse_table_operators, operator_rows, column_names=[
        'network', 'operator_id', 'operator_name', 'is_vo', 'is_private', 'validator_count', 'address', 'updated_at'
    ])

    client.insert(clickhouse_table_performance, performance_rows, column_names=[
        'network', 'operator_id', 'metric_type', 'metric_date', 'metric_value', 'source', 'updated_at'
    ])


from datetime import datetime, timedelta, timezone

def cleanup_outdated_records(client, local_time=False):
    cutoff_date = (datetime.now(timezone.utc if not local_time else None) - timedelta(days=OPERATOR_PERFORMANCE_DAYS)).strftime('%Y-%m-%d')

    # Count how many rows will be updated
    count_query = f"""
        SELECT count()
        FROM operators
        WHERE (network, operator_id) NOT IN (
            SELECT DISTINCT network, operator_id
            FROM performance
            WHERE metric_date >= toDate('{cutoff_date}')
        )
    """
    affected_rows = client.query(count_query).result_rows[0][0]  # Assuming you're using clickhouse-connect
    logging.info(f"Found {affected_rows} operators with no performance data for the last {OPERATOR_PERFORMANCE_DAYS} days. Updating validator count to zero.")

    # Now run the update
    update_query = f"""
        ALTER TABLE operators UPDATE validator_count = 0
        WHERE (network, operator_id) NOT IN (
            SELECT DISTINCT network, operator_id
            FROM performance
            WHERE metric_date >= toDate('{cutoff_date}')
        )
    """
    client.command(update_query)

    return affected_rows


# Run OPTIMIZE TABLE to deduplicate data in ClickHouse database
def deduplicate_table(client, table_name: str, network: str):
    try:
        query = f"OPTIMIZE TABLE {table_name} PARTITION %(network)s FINAL"
        client.command(query, {'network': network})
        logging.info(f"✅ Deduplicated partition '{network}' in table '{table_name}'")
    except Exception as e:
        logging.info(f"❌ Failed to deduplicate {table_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Fetch and update operator performance data.')
    parser.add_argument('-n', '--network', type=str, choices=['mainnet', 'holesky', 'hoodi'], default='mainnet')
    parser.add_argument('--page_size', type=int, default=100)
    parser.add_argument('--local_time', action='store_true')
    parser.add_argument('--ch-operators-table', default=os.environ.get('CH_OPERATORS_TABLE', 'operators'))
    parser.add_argument('--ch-performance-table', default=os.environ.get('CH_PERFORMANCE_TABLE', 'performance'))
    parser.add_argument("--log_level", default=os.environ.get("COLLECTOR_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    # Set logging level dynamically
    logging.getLogger().setLevel(args.log_level.upper())
    logging.info(f"Logging level set to {args.log_level.upper()}")

    client = get_clickhouse_client()

    base_url = f"https://api.ssv.network/api/v4/{args.network}/operators/?validatorsCount=true"
    operators = fetch_and_filter_data(base_url, args.page_size)

    target_date = datetime.now(timezone.utc if not args.local_time else None).date()

    insert_clickhouse_performance_data(client, args.network, args.ch_operators_table, args.ch_performance_table, operators, target_date, IMPORT_SOURCE)
    cleanup_outdated_records(client, args.local_time)

    deduplicate_table(client, args.ch_operators_table, args.network)
    deduplicate_table(client, args.ch_performance_table, args.network)

if __name__ == "__main__":
    main()