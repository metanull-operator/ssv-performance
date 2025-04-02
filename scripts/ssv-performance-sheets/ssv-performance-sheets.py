import argparse
import logging
import os
from datetime import date, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from clickhouse_connect import create_client

FIELD_OPERATOR_ID = 'OperatorID'
FIELD_OPERATOR_NAME = 'Name'
FIELD_IS_VO = 'isVO'
FIELD_IS_PRIVATE = 'isPrivate'
FIELD_VALIDATOR_COUNT = 'ValidatorCount'
FIELD_ADDRESS = 'Address'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Creates a two-dimensional representation of the data to be populated into the Google Sheet
def create_spreadsheet_data(data, performance_data_attribute):
    dates = sorted({d for details in data.values() for d in details[performance_data_attribute]}, reverse=True)
    sorted_entries = sorted(data.items(), key=lambda item: item[1].get(FIELD_OPERATOR_ID, 'Unknown'))

    spreadsheet_data = [['OperatorID', 'Name', 'isVO', 'isPrivate', 'ValidatorCount', 'Address'] + dates]

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


def get_operator_performance_data(network: str, days: int, metric_type: str):
    client = create_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database=os.environ.get("CLICKHOUSE_DB", "default")
    )

    since_date = (date.today() - timedelta(days=days)).isoformat()

    query = """
        SELECT 
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            o.validator_count,
            o.address,
            p.metric_date,
            p.metric_value
        FROM operators o
        LEFT JOIN performance p
            ON o.operator_id = p.operator_id AND o.network = p.network
        WHERE 
            o.network = %(network)s AND 
            p.metric_type = %(metric_type)s AND 
            p.metric_date >= %(since_date)s
        ORDER BY o.operator_id, p.metric_date
    """

    params = {'network': network, 'metric_type': metric_type, 'since_date': since_date}
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


def main():
    parser = argparse.ArgumentParser(description="Export SSV operator performance to Google Sheets")
    parser.add_argument('-c', '--discord_credentials', type=str, default=os.environ.get('GOOGLE_CREDENTIALS_FILE', '/etc/ssv-performance-sheets/credentials/google-credentials.json'))
    parser.add_argument('-d', '--document', type=str, required=True)
    parser.add_argument('-w', '--worksheet', type=str, required=True)
    parser.add_argument('-n', '--network', type=str, default='mainnet')
    parser.add_argument('--days', type=int, default=os.environ.get('NUMBER_OF_DAYS_TO_UPLOAD', 180), help='How many days of data to include')
    parser.add_argument('--metric', type=str, choices=['24h', '30d'], default='24h', help='Performance metric type')
    args = parser.parse_args()

    credentials_file = args.discord_credentials
    document_name = args.document
    worksheet_name = args.worksheet

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        gc = gspread.authorize(credentials)
        logging.info("Authenticated with Google Sheets API.")

        worksheet = gc.open(document_name).worksheet(worksheet_name)
        logging.info(f"Opened Google Sheet document: {document_name}, worksheet: {worksheet_name}.")

    except Exception as e:
        logging.error(f"Error during Google Sheets authentication or opening document: {e}")
        return

    try:
        perf_data = get_operator_performance_data(network=args.network, days=args.days, metric_type=args.metric)
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


if __name__ == "__main__":
    main()
