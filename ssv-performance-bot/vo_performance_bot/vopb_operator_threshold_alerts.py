import logging
from common.config import *


# Looks for the most recent 24h data point and returns operator details if that data point violates the threshold
def operator_threshold_alert_24h(operator, threshold):

    logging.debug(f"Operator {operator[FIELD_OPERATOR_ID]}: Checking 24h data point against threshold {threshold}")

    try:
        # If we have no data points, then there are no alerts
        if FIELD_PERFORMANCE not in operator or not operator[FIELD_PERFORMANCE]:
            logging.debug(f"Operator {operator[FIELD_OPERATOR_ID]}: No performance data available")
            return None

        # Get the most recent data point for which we have data points for this operator
        most_recent_date = operator[FIELD_PERFORMANCE_DATE]
        data_point = operator[FIELD_PERFORMANCE]['24h']

        try:
            if data_point is not None:
                data_point = float(data_point)
            else:
                return None
        except (ValueError, TypeError) as e:
            logging.warning(f"Error converting 24h data point to float for operator {operator[FIELD_OPERATOR_ID]} and date {most_recent_date}: {e}", exc_info=True)
            return None

        logging.debug(f"Operator {operator[FIELD_OPERATOR_ID]}: 24h data point: {data_point}, threshold: {threshold}")

        if data_point < threshold:
            logging.debug(f"Operator {operator[FIELD_OPERATOR_ID]}: 24h data point {data_point} is below threshold {threshold}")
            return {
                FIELD_OPERATOR_ID: operator[FIELD_OPERATOR_ID],
                FIELD_OPERATOR_NAME: operator[FIELD_OPERATOR_NAME],
                FIELD_VALIDATOR_COUNT: operator[FIELD_VALIDATOR_COUNT],
                'Performance Period': most_recent_date,
                'Performance Data Point': f"{data_point * 100:.2f}%"
            }
    except Exception as e:
        logging.error(f"Unexpected error in vo_24h_threshold_alerts(): {e}", exc_info=True)

    return None


# Looks for the most recent 30d data point and returns operator details if that data point violates the threshold
def operator_threshold_alert_30d(operator, threshold):
    try:
        # If we have no data points, then there are no alerts
        if FIELD_PERFORMANCE not in operator or not operator[FIELD_PERFORMANCE]:
            return None

        # Get the most recent data point for which we have data points for this operator
        most_recent_date = operator[FIELD_PERFORMANCE_DATE]
        data_point = operator[FIELD_PERFORMANCE]['30d']

        try:
            if data_point is not None:
                data_point = float(data_point)
            else:
                return None
        except (ValueError, TypeError) as e:
            logging.warning(f"Error converting 30d data point to float for operator {operator[FIELD_OPERATOR_ID]} and date {most_recent_date}: {e}", exc_info=True)
            return None

        if data_point < threshold:
            return {
                FIELD_OPERATOR_ID: operator[FIELD_OPERATOR_ID],
                FIELD_OPERATOR_NAME: operator[FIELD_OPERATOR_NAME],
                FIELD_VALIDATOR_COUNT: operator[FIELD_VALIDATOR_COUNT],
                'Performance Period': most_recent_date,
                'Performance Data Point': f"{data_point * 100:.2f}%"
            }
    except Exception as e:
        logging.error(f"Unexpected error in vo_30d_threshold_alerts(): {e}", exc_info=True)

    return None

