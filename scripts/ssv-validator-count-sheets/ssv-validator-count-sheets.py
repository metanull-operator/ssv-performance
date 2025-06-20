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
    FIELD_ADDRESS
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Creates a two-dimensional representation of the data to be populated into the Google Sheet
def create_spreadsheet_data(data, count_data_attribute):
    dates = sorted({d for details in data.values() for d in details[count_data_attribute]}, reverse=True)
    sorted_entries = sorted(data.items(), key=lambda item: item[1].get(FIELD_OPERATOR_ID, 'Unknown'))

    spreadsheet_data = [SPREADSHEET_COLUMNS + dates]

    for op_id, details in sorted_entries:
        row = [
            op_id,
            details.get(FIELD_OPERATOR_NAME, None),
            1 if details.get(FIELD_IS_VO, 0) else 0,
            1 if details.get(FIELD_IS_PRIVATE, 0) else 0,
            details.get(FIELD_ADDRESS, None)
        ]
        for d in dates:
            row.append(details[count_data_attribute].get(d, None))
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


def get_operator_validator_count_data(network: str, days: int, clickhouse_password: str):
    client = get_clickhouse_client(clickhouse_password=clickhouse_password)

    since_date = (date.today() - timedelta(days=days)).isoformat()

    query = """
        SELECT 
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            o.address,
            v.metric_date,
            v.validator_count
        FROM operators o
        LEFT JOIN validator_counts v
            ON o.operator_id = v.operator_id AND o.network = v.network
        WHERE 
            o.network = %(network)s AND 
            v.metric_date >= %(since_date)s
        ORDER BY o.operator_id, v.metric_date
    """

    params = {'network': network, 'since_date': since_date}
    rows = client.query(query, parameters=params).result_rows

    result = {}
    for row in rows:
        op_id = row[0]
        metric_date = row[5].strftime('%Y-%m-%d')
        if op_id not in result:
            result[op_id] = {
                FIELD_OPERATOR_ID: op_id,
                FIELD_OPERATOR_NAME: row[1],
                FIELD_IS_VO: row[2],
                FIELD_IS_PRIVATE: row[3],
                FIELD_ADDRESS: row[4],
                'validator_counts': {}
            }
        result[op_id]['validator_counts'][metric_date] = float(row[6])

    return result


def authorize_google_sheets(credentials_file):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    return gspread.authorize(credentials)


def main():
    parser = argparse.ArgumentParser(description="Export SSV validator count data to Google Sheets")
    parser.add_argument('-c', '--google_credentials_file', type=str, default=os.environ.get('GOOGLE_CREDENTIALS_FILE'))
    parser.add_argument('-p', '--clickhouse_password_file', type=str, default=os.environ.get('CLICKHOUSE_PASSWORD_FILE'))
    parser.add_argument('-d', '--document', type=str, required=True)
    parser.add_argument('-w', '--worksheet', type=str, required=True)
    parser.add_argument('-n', '--network', type=str, default=os.environ.get('NETWORK', 'mainnet'))
    parser.add_argument('--days', type=int, default=os.environ.get('NUMBER_OF_DAYS_TO_UPLOAD', 180), help='How many days of data to include')
    parser.add_argument("--log_level", default=os.environ.get("SHEETS_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    # Set logging level dynamically
    logging.getLogger().setLevel(args.log_level.upper())
    logging.info(f"Logging level set to {args.log_level.upper()}")

    clickhouse_password_file = args.clickhouse_password_file
    credentials_file = args.google_credentials_file
    document_name = args.document
    worksheet_name = args.worksheet

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
        count_data = get_operator_validator_count_data(network=args.network, days=args.days, clickhouse_password=clickhouse_password)
        logging.info("Retrieved performance data from ClickHouse.")

    except Exception as e:
        logging.error(f"Error retrieving data from ClickHouse: {e}")
        return

    try:
        spreadsheet = create_spreadsheet_data(count_data, 'validator_counts')
        worksheet.clear()
        worksheet.update(values=spreadsheet, range_name='A1', value_input_option='USER_ENTERED')
        worksheet.resize(rows=len(spreadsheet), cols=len(spreadsheet[0]))
        logging.info("Updated Google Sheet with new performance data.")

    except Exception as e:
        logging.error(f"Error during spreadsheet update: {e}")

    logging.info(f"Completed spreadsheet update for {args.network} at {datetime.utcnow().isoformat()} UTC.")


if __name__ == "__main__":
    main()
