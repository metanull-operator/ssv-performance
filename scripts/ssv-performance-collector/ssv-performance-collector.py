from clickhouse_connect import create_client
from datetime import datetime, timezone, timedelta
import requests
import argparse
import time
import os
import logging

MISSING_PERFORMANCE_DAYS = int(os.environ.get('MISSING_PERFORMANCE_DAYS', 7)) 
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 20)) # Total requests to API per minute
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE

BLOCKS_PER_DAY = 7200
DAYS_PER_YEAR = 365
BLOCKS_PER_YEAR = BLOCKS_PER_DAY * DAYS_PER_YEAR

# SSV API configuration
IMPORT_SOURCE = os.environ.get("IMPORT_SOURCE", 'api.ssv.network')

# SUBGRAPH API configuration
GRAPH_SUBGRAPH_URL = "https://gateway.thegraph.com/api/subgraphs/id/7V45fKPugp9psQjgrGsfif98gWzCyC6ChN7CW98VyQnr"
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY", "")

# BEACON API configuration``
BEACON_API_URL = os.environ.get("BEACON_API_URL", None)
STATUS_RPM = int(os.environ.get("VALIDATOR_STATUS_RPM", 30))
STATUS_BATCH_SIZE = int(os.environ.get("VALIDATOR_STATUS_BATCH", 50))
CACHE_TTL_MINUTES = int(os.environ.get("VALIDATOR_CACHE_TTL_MINUTES", 60))

STATUS_DELAY = 60 / STATUS_RPM
ACTIVE_STATUSES = {
    "active_ongoing",
    "active_exiting",
    "active_slashed",
    "pending_queued",
    "pending_initialized",
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_clickhouse_client(clickhouse_password):
    return create_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "ssv_performance"),
        password=clickhouse_password,
        database=os.environ.get("CLICKHOUSE_DB", "default")
    )


def fetch_all_operator_validators():
    op_skip = 0
    page_size = 1000
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GRAPH_API_KEY}"
    }
    operator_validators = {}
    seen_pubkeys = set()

    while True:
        query = f"""
        {{
          operators(first: {page_size}, skip: {op_skip}) {{
            id
            validators(first: 1000, where: {{removed: false}}) {{
              id
            }}
          }}
        }}
        """
        resp = requests.post(GRAPH_SUBGRAPH_URL, headers=headers, json={"query": query})
        resp.raise_for_status()
        ops = resp.json().get("data", {}).get("operators", [])
        if not ops:
            break

        for op in ops:
            op_id = int(op["id"])
            keys = {v["id"] for v in op.get("validators", [])}
            operator_validators[op_id] = keys
            seen_pubkeys.update(keys)

        op_skip += page_size
        time.sleep(1)

    return operator_validators, seen_pubkeys

# --- Query cache ---
def get_cached_statuses(client, pubkeys: set[str]) -> dict[str, str]:
    if not pubkeys:
        return {}
    chunk = "', '".join(pubkeys)
    query = f"""
        SELECT pubkey, status
        FROM validator_status
        WHERE pubkey IN ('{chunk}')
        AND last_updated > now() - INTERVAL {CACHE_TTL_MINUTES} MINUTE
    """
    result = client.query(query)
    return {row["pubkey"]: row["status"] for row in result.result_rows}


def insert_statuses(client, statuses: dict[str, str]) -> None:
    now = datetime.utcnow().replace(microsecond=0)
    rows = [(k, v, now) for k, v in statuses.items()]
    client.insert("validator_status", rows, column_names=["pubkey", "status", "last_updated"])


def fetch_validator_statuses(pubkeys: set[str]) -> dict[str, str]:
    result = {}
    pubkeys = list(pubkeys)
    for i in range(0, len(pubkeys), STATUS_BATCH_SIZE):
        batch = pubkeys[i:i+STATUS_BATCH_SIZE]
        ids = ",".join(batch)
        try:
            url = f"{BEACON_API_URL}/eth/v1/beacon/states/head/validators?id={ids}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            for v in data:
                key = v.get("validator", {}).get("pubkey") or v.get("index") or ""
                result[key] = v.get("status", "")
        except Exception as e:
            logging.warning(f"Failed to fetch validator status batch {i}: {e}")
        time.sleep(STATUS_DELAY)
    return result


def calculate_active_counts(operator_validators: dict[int, set[str]], status_map: dict[str, str]) -> dict[int, int]:
    return {
        op_id: sum(1 for k in pubkeys if status_map.get(k, "") in ACTIVE_STATUSES)
        for op_id, pubkeys in operator_validators.items()
    }


def enrich_operator_counts(client, operators: dict[int, dict]) -> None:
    if not GRAPH_API_KEY:
        logging.info("GRAPH_API_KEY not set. Skipping validator counts enrichment.")
        return

    operator_validators, all_pubkeys = fetch_all_operator_validators()
    logging.info(f"Found {len(operator_validators)} operators and {len(all_pubkeys)} unique validators.")

    cached = get_cached_statuses(client, all_pubkeys)
    missing = all_pubkeys - set(cached)

    if BEACON_API_URL and missing:
        logging.info(f"Fetching {len(missing)} validator statuses from beacon API")
        new_statuses = fetch_validator_statuses(missing)
        insert_statuses(client, new_statuses)
        cached.update(new_statuses)

    counts = calculate_active_counts(operator_validators, cached)
    for op_id, count in counts.items():
        if op_id in operators:
            operators[op_id]["activeValidatorCount"] = count


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

        if not data.get("operators"):
            break

        for op in data["operators"]:
            try:
                perf = op.get("performance", {})
                perf_24h_raw = perf.get("24h")
                perf_30d_raw = perf.get("30d")

                perf_clean = {}

                # Normalize 24h performance
                if perf_24h_raw is not None:
                    val = float(perf_24h_raw)
                    perf_clean["24h"] = val if val == 0 else val / 100

                # Normalize 30d performance
                if perf_30d_raw is not None:
                    val = float(perf_30d_raw)
                    perf_clean["30d"] = val if val == 0 else val / 100

                # Only keep if we have at least one of the two
                if perf_clean:
                    op["performance"] = perf_clean
                    operators[int(op["id"])] = op

            except Exception as e:
                logging.error(f"Error processing operator {op.get('id')}: {e}")
                continue

        page += 1
        time.sleep(REQUEST_DELAY)

    return operators


def insert_clickhouse_performance_data(client, network, clickhouse_table_operators, clickhouse_table_performance, operators, target_date, source):
    performance_rows = []
    operator_rows = []
    validator_counts_rows = []
    operator_fees_rows = []

    now = datetime.now(timezone.utc)

    for operator_id, operator in operators.items():
        performance_24h = operator["performance"]['24h']
        performance_30d = operator["performance"]['30d']
        validator_count = operator.get("validators_count", None)

        is_vo = 1 if operator.get("type", "") == "verified_operator" else 0
        is_private = 1 if operator.get("is_private", False) else 0

        operator_fee = operator.get("fee", None)
        if operator_fee is not None:
            operator_fee = float(operator_fee)
            operator_fee = (operator_fee * BLOCKS_PER_YEAR) / 1e18

        operator_rows.append((
            network,
            operator_id,
            operator.get("name", ""),
            is_vo,
            is_private,
            validator_count,
            operator_fee,
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

        if operator_fee is not None:
            operator_fees_rows.append((
                network,
                operator_id,
                target_date,
                operator_fee,
                source,
                datetime.now(timezone.utc)
            ))

        if validator_count is not None:
            validator_counts_rows.append((
                network,
                operator_id,
                target_date,
                validator_count,
                source,
                datetime.now(timezone.utc)
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

    logging.info(f"Inserting/updating {len(operator_rows)} operator records, {len(performance_rows)} performance records, {len(validator_counts_rows)} validator count records, and {len(operator_fees_rows)} operator fee records into database.")

    client.insert(clickhouse_table_operators, operator_rows, column_names=[
        'network', 'operator_id', 'operator_name', 'is_vo', 'is_private', 'validator_count', 'operator_fee', 'address', 'updated_at'
    ])

    client.insert(clickhouse_table_performance, performance_rows, column_names=[
        'network', 'operator_id', 'metric_type', 'metric_date', 'metric_value', 'source', 'updated_at'
    ])

    client.insert('operator_fees', operator_fees_rows, column_names=[
        'network', 'operator_id', 'metric_date', 'operator_fee', 'source', 'updated_at'
    ])

    client.insert('validator_counts', validator_counts_rows, column_names=[
        'network', 'operator_id', 'metric_date', 'validator_count', 'source', 'updated_at'
    ])    


def cleanup_outdated_records(client):
    logging.info(f"Looking for operators with no performance data in the last {MISSING_PERFORMANCE_DAYS} days...")

    # Count operators with stale or missing performance
    count_query = f"""
        SELECT count()
        FROM operators o
        LEFT JOIN (
            SELECT
                network,
                operator_id,
                max(metric_date) AS max_date
            FROM performance
            GROUP BY network, operator_id
        ) p ON o.network = p.network AND o.operator_id = p.operator_id
        WHERE max_date < today() - INTERVAL {MISSING_PERFORMANCE_DAYS} DAY OR max_date IS NULL
    """
    affected_rows = client.query(count_query).result_rows[0][0]
    logging.info(f"Found {affected_rows} operators to update across all networks.")

    # Update validator_count = 0 for operators with stale or missing performance
    update_query = f"""
        ALTER TABLE operators
        UPDATE validator_count = 0
        WHERE (network, operator_id) IN (
            SELECT o.network, o.operator_id
            FROM operators o
            LEFT JOIN (
                SELECT
                    network,
                    operator_id,
                    max(metric_date) AS max_date
                FROM performance
                GROUP BY network, operator_id
            ) p ON o.network = p.network AND o.operator_id = p.operator_id
            WHERE max_date < today() - INTERVAL {MISSING_PERFORMANCE_DAYS} DAY OR max_date IS NULL
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


def read_clickhouse_password_from_file(password_file_path):
    with open(password_file_path, 'r') as file:
        return file.read().strip()


def main():
    parser = argparse.ArgumentParser(description='Fetch and update operator performance data.')
    parser.add_argument('-n', '--network', type=str, choices=['mainnet', 'holesky', 'hoodi'], default='mainnet')
    parser.add_argument('-p', '--clickhouse_password_file', type=str, default=os.environ.get('CLICKHOUSE_PASSWORD_FILE'))
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

    clickhouse_password_file = args.clickhouse_password_file

    try:
        clickhouse_password = read_clickhouse_password_from_file(clickhouse_password_file)
    except Exception as e:
        logging.info("Unable to retrieve ClickHouse password from file, trying environment variable instead.")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")

    client = get_clickhouse_client(clickhouse_password)

    base_url = f"https://api.ssv.network/api/v4/{args.network}/operators/?"
    operators = fetch_and_filter_data(base_url, args.page_size)
    enrich_operator_counts(client, operators)

    target_date = datetime.now(timezone.utc if not args.local_time else None).date()

    insert_clickhouse_performance_data(client, args.network, args.ch_operators_table, args.ch_performance_table, operators, target_date, IMPORT_SOURCE)
    cleanup_outdated_records(client)

    deduplicate_table(client, args.ch_operators_table, args.network)
    deduplicate_table(client, args.ch_performance_table, args.network)
    deduplicate_table(client, 'operator_fees', args.network)
    deduplicate_table(client, 'validator_counts', args.network)


if __name__ == "__main__":
    main()