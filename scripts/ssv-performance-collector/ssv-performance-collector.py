from clickhouse_connect import create_client
from datetime import datetime, timezone, timedelta
import requests
import argparse
import time
import os
import logging

REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 20)) # Total requests to API per minute
REQUEST_DELAY = 60 / REQUESTS_PER_MINUTE

BLOCKS_PER_DAY = 7200
DAYS_PER_YEAR = 365
BLOCKS_PER_YEAR = BLOCKS_PER_DAY * DAYS_PER_YEAR

IMPORT_SOURCE = os.environ.get("IMPORT_SOURCE", 'api.ssv.network')
SSV_API_BASE = "https://api.ssv.network/api/v4"

ACTIVE_STATUSES = {
    "active",             # This is the main active status returned by the API
    "active_ongoing",     # This and the following are official statuses not presently returned by the API
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


def http_get_json(url: str, timeout: int = 30) -> dict | None:
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logging.error(f"API request failed for {url}: {e}")
        return None


def collect_operators_from_validators(network: str, per_page: int = 1000) -> dict[int, dict]:
    """
    Fetch validators via lastId cursor and aggregate operator info & active counts.

    For each operator we produce a dict like:
      {
        "id": <int>,
        "name": <str>,
        "type": <str>,             # e.g., "verified_operator"
        "is_private": <bool>,
        "fee": <str|float|None>,   # raw from API; converted later in insert fn
        "owner_address": <str>,
        "performance": {"24h": <float>, "30d": <float>},
        "validators_count": <int>, # active validator count
      }
    """
    operators: dict[int, dict] = {}
    op_total_pubkeys: dict[int, set[str]] = {}
    op_active_pubkeys: dict[int, set[str]] = {}

    last_id: int | None = None
    batch = 0

    logging.info(f"SSV_API: Fetching validators for {network} via lastId cursor, perPage={per_page}")

    while True:
        # Create API call query
        qs = f"perPage={per_page}"
        if last_id is not None:
            qs += f"&lastId={last_id}"
        url = f"{SSV_API_BASE}/{network}/validators?{qs}"

        # Fetch data from API
        data = http_get_json(url, timeout=30)
        if data is None:
            logging.error(f"SSV_API: Stopping fetch due to request error (lastId={last_id}).")
            break

        # Stop if we no longer receive validators
        validators = data.get("validators", []) or []
        if not validators:
            logging.info(f"SSV_API: No validators returned for lastId={last_id}; stopping.")
            break

        batch += 1

        max_id_in_batch: int | None = None

        for v in validators:
            # Track highest validator ID in this batch
            vid_raw = v.get("id")
            try:
                vid = int(vid_raw) if vid_raw is not None else None
            except Exception:
                vid = None
            if vid is not None:
                if max_id_in_batch is None or vid > max_id_in_batch:
                    max_id_in_batch = vid

            # Get validator public key
            pubkey = v.get("public_key")
            if not pubkey:
                continue

            # Get validator status and determine if it's active
            st = (v.get("status") or "").lower()
            is_active = st in ACTIVE_STATUSES

            # Cycle through operators in the validator
            for op in (v.get("operators") or []):

                # Get operator ID
                raw_id = op.get("id", op.get("id_str"))
                try:
                    op_id = int(raw_id)
                except Exception:
                    continue

                # Add to lists of total pubkeys and active pubkeys
                op_total_pubkeys.setdefault(op_id, set()).add(pubkey)
                if is_active:
                    op_active_pubkeys.setdefault(op_id, set()).add(pubkey)

                # Initialize operator if not seen before
                if op_id not in operators:
                    operators[op_id] = {
                        "id": op_id,
                        "name": op.get("name", ""),
                        "type": op.get("type", ""),
                        "is_private": bool(op.get("is_private", False)),
                        "fee": op.get("fee"),
                        "owner_address": op.get("owner_address", ""),
                        "performance": {},  
                    }

                # Attach/normalize performance if present and not yet set
                if "performance" in op and isinstance(op["performance"], dict):
                    perf = operators[op_id].get("performance") or {}
                    if "24h" not in perf and op["performance"].get("24h") is not None:
                        try:
                            val = float(op["performance"]["24h"])
                            perf["24h"] = val if val == 0 else val / 100.0
                        except Exception:
                            pass
                    if "30d" not in perf and op["performance"].get("30d") is not None:
                        try:
                            val = float(op["performance"]["30d"])
                            perf["30d"] = val if val == 0 else val / 100.0
                        except Exception:
                            pass
                    operators[op_id]["performance"] = perf

        # Use pagination to determine next lastId
        pag = data.get("pagination") or {}
        next_last = pag.get("current_last")
        try:
            next_last = int(next_last) if next_last is not None else None
        except Exception:
            next_last = None

        # Fallback to max_id_in_batch if next_last is None
        if next_last is None:
            next_last = max_id_in_batch

        # Done if there is no next last ID
        if next_last is None:
            logging.info("SSV_API: Could not determine next lastId; stopping.")
            break

        # Done if the next lastId is not advancing
        if last_id is not None and next_last <= last_id:
            logging.warning(f"SSV_API: Non-advancing lastId detected (prev={last_id}, next={next_last}); stopping.")
            break

        last_id = next_last

        logging.info(
            f"SSV_API: Batch {batch} → +{len(validators)} validators; "
            f"next lastId={last_id} (operators so far: {len(operators)})"
        )

        time.sleep(REQUEST_DELAY)

    # Finalize operator data with counts
    for op_id, op in operators.items():
        # Get active validator count
        active_count = len(op_active_pubkeys.get(op_id, set()))
        op["validators_count"] = active_count

        # Default zero performance if not set
        perf = op.get("performance") or {}
        if "24h" not in perf:
            perf["24h"] = 0.0
        if "30d" not in perf:
            perf["30d"] = 0.0
        op["performance"] = perf

    logging.info(f"SSV_API: Aggregation complete. Operators={len(operators)}")
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
            try:
                operator_fee = float(operator_fee)
                operator_fee = (operator_fee * BLOCKS_PER_YEAR) / 1e18
            except Exception:
                operator_fee = None

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
    parser.add_argument('--page_size', type=int, default=1000)
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

    operators = collect_operators_from_validators(args.network, args.page_size)

    target_date = datetime.now(timezone.utc if not args.local_time else None).date()

    insert_clickhouse_performance_data(client, args.network, args.ch_operators_table, args.ch_performance_table, operators, target_date, IMPORT_SOURCE)
    
    deduplicate_table(client, args.ch_operators_table, args.network)
    deduplicate_table(client, args.ch_performance_table, args.network)
    deduplicate_table(client, 'operator_fees', args.network)
    deduplicate_table(client, 'validator_counts', args.network)


if __name__ == "__main__":
    main()