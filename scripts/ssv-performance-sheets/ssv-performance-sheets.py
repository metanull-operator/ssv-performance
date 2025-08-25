import argparse
import logging
import os
from datetime import date, datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from clickhouse_connect import create_client

FIELD_OPERATOR_ID = 'OperatorID'
FIELD_OPERATOR_NAME = 'Name'
FIELD_IS_VO = 'isVO'
FIELD_IS_PRIVATE = 'isPrivate'
FIELD_VALIDATOR_COUNT = 'ValidatorCount'
FIELD_ADDRESS = 'Address'

SPREADSHEET_COLUMNS = [
    FIELD_OPERATOR_ID,
    FIELD_OPERATOR_NAME,
    FIELD_IS_VO,
    FIELD_IS_PRIVATE,
    FIELD_VALIDATOR_COUNT,
    FIELD_ADDRESS
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Creates a two-dimensional representation of the data to be populated into the Google Sheet
def create_spreadsheet_data(data, performance_data_attribute):
    dates = sorted({d for details in data.values() for d in details[performance_data_attribute]}, reverse=True)
    sorted_entries = sorted(data.items(), key=lambda item: item[1].get(FIELD_OPERATOR_ID, 'Unknown'))

    spreadsheet_data = [SPREADSHEET_COLUMNS + dates]

    for op_id, details in sorted_entries:
        row = [
            op_id,
            details.get(FIELD_OPERATOR_NAME, None),
            1 if details.get(FIELD_IS_VO, 0) else 0,
            1 if details.get(FIELD_IS_PRIVATE, 0) else 0,
            details.get(FIELD_VALIDATOR_COUNT, None),
            details.get(FIELD_ADDRESS, None)
        ]
        for d in dates:
            row.append(details[performance_data_attribute].get(d, None))
        spreadsheet_data.append(row)

    return spreadsheet_data


def read_clickhouse_password_from_file(password_file_path):
    with open(password_file_path, 'r') as file:
        return file.read().strip()
    

def get_clickhouse_client(clickhouse_password):
    return create_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "ssv_performance"),
        password=clickhouse_password,
        database=os.environ.get("CLICKHOUSE_DB", "default")
    )


def _updated_after(self, max_age_days: int | None) -> datetime:
    """
    Convert 'days' into an absolute timestamp for filtering updated_at.
    0 or None (when default is 0) => epoch (i.e., include everything).
    """
    days = self.default_max_age_days if max_age_days is None else int(max_age_days)
    if days <= 0:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timedelta(days=days)


def get_operator_performance_data(network: str, days: int, metric_type: str, clickhouse_password: str, max_age_days: int | None = None):
    client = get_clickhouse_client(clickhouse_password=clickhouse_password)

    since_date = (date.today() - timedelta(days=days)).isoformat()

    query = """
        WITH latest_counts AS (
            SELECT
                network,
                operator_id,
                argMax(validator_count, updated_at) AS validator_count
            FROM validators_counts
            WHERE network = %(network)s
            AND updated_at >= %(updated_after)s   -- bind to your freshness threshold
            GROUP BY network, operator_id
        )
        SELECT 
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            lc.validator_count AS validator_count,   -- now from validators_counts
            o.address,
            p.metric_date,
            p.metric_value
        FROM operators AS o
        LEFT JOIN latest_counts AS lc
            ON lc.network = o.network
            AND lc.operator_id = o.operator_id
        LEFT JOIN performance AS p
            ON p.operator_id = o.operator_id
            AND p.network = o.network
            AND p.metric_type = %(metric_type)s
            AND p.metric_date >= %(since_date)s
        WHERE o.network = %(network)s
        ORDER BY o.operator_id, p.metric_date
    """

    params = {
        'network': network,
        'metric_type': metric_type,
        'since_date': since_date,
        'updated_after': _updated_after(max_age_days),
    }
    rows = client.query(query, parameters=params).result_rows

    result = {}
    for row in rows:
        op_id = row[0]
        metric_date = row[6].strftime('%Y-%m-%d')
        if op_id not in result:
            result[op_id] = {
                FIELD_OPERATOR_ID: op_id,
                FIELD_OPERATOR_NAME: row[1],
                FIELD_IS_VO: row[2],
                FIELD_IS_PRIVATE: row[3],
                FIELD_VALIDATOR_COUNT: row[4],
                FIELD_ADDRESS: row[5],
                metric_type: {}
            }
        result[op_id][metric_type][metric_date] = float(row[7])

    return result


def authorize_google_sheets(credentials_file):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    return gspread.authorize(credentials)


def main():
    parser = argparse.ArgumentParser(description="Export SSV operator performance to Google Sheets")
    parser.add_argument('-c', '--google_credentials', type=str, default=os.environ.get('GOOGLE_CREDENTIALS_FILE', '/etc/ssv-performance-sheets/credentials/google-credentials.json'))
    parser.add_argument('-p', '--clickhouse_password', type=str, default=os.environ.get('CLICKHOUSE_PASSWORD_FILE', '/etc/ssv-performance-sheets/credentials/clickhouse-password.txt'))
    parser.add_argument('--max_age_days', type=int, default=os.environ.get('MAX_AGE_DAYS', 7), help='Max age in days for data freshness filtering (0 or None means no limit)')
    parser.add_argument('-d', '--document', type=str, required=True)
    parser.add_argument('-w', '--worksheet', type=str, required=True)
    parser.add_argument('-n', '--network', type=str, default='mainnet')
    parser.add_argument('--days', type=int, default=os.environ.get('NUMBER_OF_DAYS_TO_UPLOAD', 180), help='How many days of data to include')
    parser.add_argument('--metric', type=str, choices=['24h', '30d'], default='24h', help='Performance metric type')
    parser.add_argument("--log_level", default=os.environ.get("SHEETS_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    # Set logging level dynamically
    logging.getLogger().setLevel(args.log_level.upper())
    logging.info(f"Logging level set to {args.log_level.upper()}")

    clickhouse_password_file = args.clickhouse_password
    credentials_file = args.google_credentials
    document_name = args.document
    worksheet_name = args.worksheet
    max_age_days = args.max_age_days

    try:
        clickhouse_password = read_clickhouse_password_from_file(clickhouse_password_file)
    except Exception as e:
        logging.info("Unable to retrieve ClickHouse password from file, trying environment variable instead.")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")

    try:
        gc = authorize_google_sheets(credentials_file)
        logging.info("Authenticated with Google Sheets API.")

        worksheet = gc.open(document_name).worksheet(worksheet_name)
        logging.info(f"Opened Google Sheet document: {document_name}, worksheet: {worksheet_name}.")

    except Exception as e:
        logging.error(f"Error during Google Sheets authentication or opening document: {e}")
        return

    try:
        perf_data = get_operator_performance_data(network=args.network, days=args.days, metric_type=args.metric, clickhouse_password=clickhouse_password, max_age_days=max_age_days)
        logging.info("Retrieved performance data from ClickHouse.")

    except Exception as e:
        logging.error(f"Error retrieving data from ClickHouse: {e}")
        return

    try:
        spreadsheet = create_spreadsheet_data(perf_data, args.metric)
        worksheet.clear()
        worksheet.update(values=spreadsheet, range_name='A1', value_input_option='USER_ENTERED')
        worksheet.resize(rows=len(spreadsheet), cols=len(spreadsheet[0]))
        logging.info("Updated Google Sheet with new performance data.")

    except Exception as e:
        logging.error(f"Error during spreadsheet update: {e}")

    logging.info(f"Completed spreadsheet update for {args.network} at {datetime.utcnow().isoformat()} UTC.")


if __name__ == "__main__":
    main()
