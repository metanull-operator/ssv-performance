from clickhouse_connect import create_client
from datetime import datetime, timezone
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

BEACON_API_URL     = os.environ.get("BEACON_API_URL")
STATUS_RPM         = int(os.environ.get("VALIDATOR_STATUS_RPM", 60))
STATUS_BATCH_SIZE  = int(os.environ.get("VALIDATOR_STATUS_BATCH", 1000))
STATUS_DELAY       = 60 / max(1, STATUS_RPM)

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


def fetch_operators_from_ssv(network: str, per_page: int = 100) -> dict[int, dict]:
    operators: dict[int, dict] = {}

    page = 1
    while True:
        url = f"{SSV_API_BASE}/{network}/operators?perPage={per_page}&page={page}"
        data = http_get_json(url, timeout=30)
        if data is None:
            logging.error(f"SSV_API: Stopping operators fetch due to request error at page={page}.")
            break

        ops = data.get("operators", []) or []
        if not ops:
            break

        for op in ops:
            try:
                op_id = int(op["id"])
            except Exception:
                continue

            # Normalize performance
            perf = {}
            p = op.get("performance") or {}
            try:
                v = p.get("24h")
                if v is not None:
                    v = float(v)
                    perf["24h"] = v if v == 0 else v / 100.0
            except Exception:
                pass
            try:
                v = p.get("30d")
                if v is not None:
                    v = float(v)
                    perf["30d"] = v if v == 0 else v / 100.0
            except Exception:
                pass
            if "24h" not in perf: perf["24h"] = 0.0
            if "30d" not in perf: perf["30d"] = 0.0

            operators[op_id] = {
                "id": op_id,
                "name": op.get("name", ""),
                "type": op.get("type", ""),
                "is_private": bool(op.get("is_private", False)),
                "fee": op.get("fee"),
                "owner_address": op.get("owner_address", ""),
                "performance": perf,
                # We'll fill validators_count later
            }

        logging.info("SSV_API: Operators page %d → +%d (total: %d)", page, len(ops), len(operators))
        page += 1
        time.sleep(REQUEST_DELAY)

    logging.info("SSV_API: Collected %d operators from /operators.", len(operators))
    return operators


def fetch_validators_maps(network: str, per_page: int = 1000):
    """
    Cursor-based pagination using lastId for /validators.
    Returns:
      - operator_validators: {op_id -> set(pubkeys)}
      - all_pubkeys: set(pubkeys)
      - all_pubkeys_status: {pubkey -> bool}
    """
    operator_validators: dict[int, set[str]] = {}
    all_pubkeys: set[str] = set()
    all_pubkeys_status: dict[str, bool] = {}

    last_id: int | None = None
    batch = 0

    logging.info("SSV_API: Fetching validators via lastId, perPage=%d", per_page)

    while True:
        qs = f"perPage={per_page}"
        if last_id is not None:
            qs += f"&lastId={last_id}"
        url = f"{SSV_API_BASE}/{network}/validators?{qs}"

        data = http_get_json(url, timeout=30)
        if data is None:
            logging.error(f"Stopping validators fetch due to request error (lastId={last_id}).")
            break

        validators = data.get("validators", []) or []
        if not validators:
            logging.info("SSV_API: No validators for lastId=%s; stopping.", last_id)
            break

        batch += 1
        max_id_in_batch: int | None = None

        for v in validators:

            # Tracking last ID retrieved from API for next query cursor
            vid_raw = v.get("id")
            try:
                vid = int(vid_raw) if vid_raw is not None else None
            except Exception:
                vid = None
            if vid is not None:
                if max_id_in_batch is None or vid > max_id_in_batch:
                    max_id_in_batch = vid

            # Get a pubkey or move along
            pubkey = v.get("public_key")
            if not pubkey:
                logging.warning(f"SSV_API: Validator with missing/empty pubkey (id={vid_raw}); skipping.")
                continue
            
            # Make sure all pubkeys are 0x-prefixed
            if isinstance(pubkey, str) and not pubkey.startswith("0x"):
                pubkey = "0x" + pubkey.strip()

            # Add to lists of all validators and active validators
            all_pubkeys.add(pubkey)

            # Prefer specific validator status or general is_active flag
            st = ((v.get("validator_info") or {}).get("status") or "").lower()
            if st is not None and st != "":
                all_pubkeys_status[pubkey] = st
            else:
                all_pubkeys_status[pubkey] = "active"

            # Build map of operators to their validators
            #   Dict with operator ID key and valus is a set of pubkeys
            for op in (v.get("operators") or []):
                raw_id = op.get("id", op.get("id_str"))
                try:
                    op_id = int(raw_id)
                except Exception:
                    logging.warning(f"SSV_API: Validator {pubkey} has invalid operator ID: {raw_id}; skipping this operator.")
                    continue
                operator_validators.setdefault(op_id, set()).add(pubkey)

        # Advance cursor
        pag = data.get("pagination") or {}
        next_last = pag.get("current_last")
        try:
            next_last = int(next_last) if next_last is not None else None
        except Exception:
            next_last = None
        if next_last is None:
            next_last = max_id_in_batch

        if next_last is None:
            logging.info("SSV_API: Could not determine next lastId; stopping.")
            break

        if last_id is not None and next_last <= last_id:
            logging.warning("SSV_API: Non-advancing lastId (prev=%s, next=%s); stopping.", last_id, next_last)
            break

        last_id = next_last
        logging.info("SSV_API: Batch %d → +%d validators; next lastId=%s (operators with validators so far: %d)",
                     batch, len(validators), last_id, len(operator_validators))

        time.sleep(REQUEST_DELAY)

    logging.info("SSV_API: Validators done. Unique validators=%d, operators_with_validators=%d",
                 len(all_pubkeys), len(operator_validators))
    
    return operator_validators, all_pubkeys, all_pubkeys_status


def fetch_beacon_statuses(pubkeys: set[str]) -> dict[str, str]:
    """
    Fetch statuses from Beacon once per pubkey (batches).
    Returns {pubkey: status_lower}.
    """
    if not BEACON_API_URL:
        return {}

    headers = {"Accept": "application/json"}
    pubkey_list = list(pubkeys)
    result: dict[str, str] = {}

    logging.info("BEACON_API: Requesting statuses for %d validators", len(pubkey_list))
    for i in range(0, len(pubkey_list), STATUS_BATCH_SIZE):
        batch = pubkey_list[i:i + STATUS_BATCH_SIZE]
        ids = ",".join(batch)
        url = f"{BEACON_API_URL}/eth/v1/beacon/states/head/validators?id={ids}"
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            validators = resp.json().get("data", []) or []
            # Map pubkey -> status
            for item in validators:
                pk = (item.get("validator") or {}).get("pubkey", "")
                st = (item.get("status") or "").lower()
                if pk:
                    result[pk] = st

            # If fewer returned than requested, the missing ones likely aren't on-chain/deposited.
            if len(validators) < len(batch):
                logging.info("BEACON_API: batch %d-%d returned %d/%d records.",
                              i, i + STATUS_BATCH_SIZE, len(validators), len(batch))
        except Exception as e:
            logging.warning("BEACON_API: Failed batch %d-%d: %s", i, i + STATUS_BATCH_SIZE, e)

        if i % (STATUS_BATCH_SIZE * 10) == 0:
            logging.info("BEACON_API: processed %d / %d", i + len(batch), len(pubkey_list))

        time.sleep(STATUS_DELAY)

    logging.info("BEACON_API: Retrieved statuses for %d validators", len(result))
    return result


def count_active_from_status_map(operator_validators: dict[int, set[str]], status_map: dict[str, str]) -> dict[int, int]:
    return {
        op_id: sum(1 for pk in pubkeys if status_map.get(pk, "") in ACTIVE_STATUSES)
        for op_id, pubkeys in operator_validators.items()
    }


def insert_clickhouse_performance_data(client, network, clickhouse_table_operators, clickhouse_table_performance, operators, target_date, source):
    performance_rows = []
    operator_rows = []
    validator_counts_rows = []
    operator_fees_rows = []

    now = datetime.now(timezone.utc)

    for operator_id, operator in operators.items():
        perf_24h = (operator.get("performance") or {}).get("24h", 0.0)
        perf_30d = (operator.get("performance") or {}).get("30d", 0.0)

        validator_count = operator.get("validators_count", None)  # Fallback to None, failsafe

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

        performance_rows.append((network, operator_id, '24h', target_date, perf_24h, source, now))
        performance_rows.append((network, operator_id, '30d', target_date, perf_30d, source, now))

        if operator_fee is not None:
            operator_fees_rows.append((network, operator_id, target_date, operator_fee, source, now))

        if validator_count is not None:
            validator_counts_rows.append((network, operator_id, target_date, validator_count, source, now))

    logging.info("CLICKHOUSE: inserting %d operators, %d perf rows, %d validator count rows, %d fee rows",
                 len(operator_rows), len(performance_rows), len(validator_counts_rows), len(operator_fees_rows))

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


def insert_clickhouse_validator_count_data(client, network, validator_counts, target_date, source):
    validator_counts_rows = []

    now = datetime.now(timezone.utc)

    for operator_id, validator_count in validator_counts.items():

        operator_rows.append((
            network,
            operator_id,
            metric_date,
            validator_count,
            source,
            now
        ))

        if validator_count is not None:
            validator_counts_rows.append((network, operator_id, target_date, validator_count, source, now))

    logging.info("CLICKHOUSE: inserting %d operators, %d perf rows, %d validator count rows, %d fee rows",
                 len(operator_rows), len(performance_rows), len(validator_counts_rows), len(operator_fees_rows))

    client.insert(clickhouse_table_operators, operator_rows, column_names=[
        'network', 'operator_id', 'operator_name', 'is_vo', 'is_private', 'validator_count', 'operator_fee', 'address', 'updated_at'
    ])

    client.insert(clickhouse_table_performance, performance_rows, column_names=[
        'network', 'operator_id', 'metric_type', 'metric_date', 'metric_value', 'source', 'updated_at'
    ])

    client.insert('operator_fees', operator_fees_rows, column_names=[
        'network', 'operator_id', 'metric_date', 'operator_fee', 'source', 'updated_at'
    ])


def deduplicate_table(client, table_name: str, network: str):
    try:
        query = f"OPTIMIZE TABLE {table_name} PARTITION %(network)s FINAL"
        client.command(query, {'network': network})
        logging.info("Deduplicated partition '%s' in table '%s'", network, table_name)
    except Exception as e:
        logging.warning("Failed to deduplicate %s: %s", table_name, e)


def read_clickhouse_password_from_file(password_file_path):
    with open(password_file_path, 'r') as file:
        return file.read().strip()


def main():
    parser = argparse.ArgumentParser(description='Fetch/update operator data via operators + validators; optional Beacon cross-check.')
    parser.add_argument('-n', '--network', type=str, choices=['mainnet', 'holesky', 'hoodi'], default='mainnet')
    parser.add_argument('-p', '--clickhouse_password_file', type=str, default=os.environ.get('CLICKHOUSE_PASSWORD_FILE'))
    parser.add_argument('--ops-page-size', type=int, default=100, help='perPage for /operators')
    parser.add_argument('--val-page-size', type=int, default=1000, help='perPage for /validators (lastId cursor)')
    parser.add_argument('--local_time', action='store_true')
    parser.add_argument('--ch-operators-table', default=os.environ.get('CH_OPERATORS_TABLE', 'operators'))
    parser.add_argument('--ch-performance-table', default=os.environ.get('CH_PERFORMANCE_TABLE', 'performance'))
    parser.add_argument("--log_level", default=os.environ.get("COLLECTOR_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()

    # Set logging level dynamically
    logging.getLogger().setLevel(args.log_level.upper())
    logging.info(f"Logging level set to {args.log_level.upper()}")

    try:
        clickhouse_password = read_clickhouse_password_from_file(args.clickhouse_password_file)
    except Exception:
        logging.info("Unable to read ClickHouse password file; trying CLICKHOUSE_PASSWORD env.")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")

    # Step 1: full operators list (metadata)
    operators = fetch_operators_from_ssv(args.network, args.ops_page_size)

    # Step 2: Query validators endpoint and optionally beacon API for statuses
    operator_validators, all_pubkeys, all_pubkeys_status = fetch_validators_maps(args.network, args.val_page_size)
    logging.info("SSV_API: Operators with > 0 validators: %d/%d total).",
                 len([k for k,v in operator_validators.items() if v]), len(operators))

    # Step 3: If BEACON_API_URL set, fetch statuses and use those counts instead of SSV-based
    final_active_counts: dict[int, int] = {}
    if BEACON_API_URL:
        logging.info("Getting BEACON_API validator statuses")
        final_active_counts = count_active_from_status_map(operator_validators, fetch_beacon_statuses(all_pubkeys))
    else:
        logging.info("No BEACON_API_URL set; using SSV-based active counts.")
        final_active_counts = count_active_from_status_map(operator_validators, all_pubkeys_status)

    # Set the final active count into operators[op]['validators_count'] (used by DB writer)
    for op_id, op in operators.items():
        op["validators_count"] = final_active_counts.get(op_id, 0)

    target_date = datetime.now(timezone.utc if not args.local_time else None).date()

    client = get_clickhouse_client(clickhouse_password)

    insert_clickhouse_performance_data(client, args.network, args.ch_operators_table, args.ch_performance_table, operators, target_date, IMPORT_SOURCE)
    insert_clickhouse_validator_count_data(client, args.network, final_active_counts, target_date, IMPORT_SOURCE)

    deduplicate_table(client, args.ch_operators_table, args.network)
    deduplicate_table(client, args.ch_performance_table, args.network)
    deduplicate_table(client, 'operator_fees', args.network)
    deduplicate_table(client, 'validator_counts', args.network)


if __name__ == "__main__":
    main()