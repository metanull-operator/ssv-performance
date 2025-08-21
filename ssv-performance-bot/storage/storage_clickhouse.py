import time
import os
import logging
from common.config import *
from .storage_data_interface import DataStorageInterface
from datetime import datetime, timezone
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import ClickHouseError

class ClickHouseStorage(DataStorageInterface):

    def __init__(self, retries=5, delay=2, **kwargs):
        for attempt in range(1, retries + 1):
            try:
                self.client = create_client(
                    host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
                    port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
                    username=os.environ.get("CLICKHOUSE_USER", "ssv_performance"),
                    password=kwargs.get("password"),
                    database=os.environ.get("CLICKHOUSE_DB", "default")
                )
                break  # Success!
            except ClickHouseError as e:
                print(f"⚠️ Attempt {attempt} failed to connect to ClickHouse: {e}")
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise  # Raise the last exception after exhausting retries


    # Get all performance data
    def get_performance_all(self, network):
        query = """
            SELECT *
            FROM performance
            WHERE network = %(network)s
        """
        params = {'network': network}
        rows = self.client.query(query, parameters=params).result_rows

        perf_data = {}
        for row in rows:
            perf_data[row[FIELD_OPERATOR_ID]] = row

        return perf_data


    def get_latest_fee_data(self, network):
        query = """
            SELECT 
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                o.validator_count,
                o.address,
                o.operator_fee,
                o.updated_at
            FROM operators o
            WHERE o.network = %(network)s
        """

        params = {
            'network': network        # e.g., 'mainnet'
        }

        rows = self.client.query(query, parameters=params).result_rows

        fee_data = {}
        for row in rows:
            operator_id = row[0]
            if operator_id not in fee_data:
                fee_data[operator_id] = {
                    FIELD_OPERATOR_ID: row[0],
                    FIELD_OPERATOR_NAME: row[1],
                    FIELD_IS_VO: row[2],
                    FIELD_IS_PRIVATE: row[3],
                    FIELD_VALIDATOR_COUNT: row[4],
                    FIELD_ADDRESS: row[5],
                    FIELD_OPERATOR_FEE: row[6],
                    FIELD_OPERATOR_FEE_DATE: row[7]
                }

        return fee_data


    def get_latest_performance_data(self, network, period):
        query = """
            WITH latest AS (
            SELECT MAX(metric_date) AS max_date
            FROM performance
            WHERE metric_type = %(metric_type)s
                AND network     = %(network)s
            )
            SELECT 
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                o.validator_count,
                o.address,
                pm.metric_date,
                pm.metric_value
            FROM operators o
            LEFT JOIN (
            SELECT *
            FROM (
                SELECT
                p.operator_id,
                p.network,
                p.metric_date,
                p.metric_value,
                ROW_NUMBER() OVER (
                    PARTITION BY p.network, p.operator_id
                    ORDER BY p.metric_date DESC
                ) AS rn
                FROM performance p
                JOIN latest l ON p.metric_date = l.max_date
                WHERE p.metric_type = %(metric_type)s
                AND p.network     = %(network)s
            ) x
            WHERE x.rn = 1
            ) pm
            ON o.operator_id = pm.operator_id
            AND o.network     = pm.network
            WHERE o.network = %(network)s;
        """

        params = {
            'metric_type': period,    # e.g., '30d'
            'network': network        # e.g., 'mainnet'
        }

        rows = self.client.query(query, parameters=params).result_rows

        perf_data = {}
        for row in rows:
            operator_id = row[0]
            if operator_id not in perf_data:
                perf_data[operator_id] = {
                    FIELD_OPERATOR_ID: row[0],
                    FIELD_OPERATOR_NAME: row[1],
                    FIELD_IS_VO: row[2],
                    FIELD_IS_PRIVATE: row[3],
                    FIELD_VALIDATOR_COUNT: row[4],
                    FIELD_ADDRESS: row[5],
                    FIELD_PERFORMANCE_DATE: row[6],
                    FIELD_PERFORMANCE: { period: row[7] }
                }
#            date_str = row[6].strftime('%Y-%m-%d')
#            perf_data[operator_id][date_str] = { period: row[7] }

        return perf_data


    # Get performance data for specific operator IDs
    def get_performance_by_opids(self, network, op_ids):
#        op_ids_str = ','.join(map(str, op_ids))
        query = """
            SELECT 
                pd.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                o.validator_count,
                o.address,
                pd.metric_date,
                pd.metric_value,
                pd.metric_type,
                o.operator_fee
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY network, operator_id, metric_type 
                        ORDER BY metric_date DESC
                    ) AS rn
                FROM performance
                WHERE 
                    network = %(network)s
                    AND operator_id IN %(operator_ids)s
            ) pd
            LEFT JOIN operators o 
                ON pd.network = o.network 
                AND pd.operator_id = o.operator_id
            WHERE pd.rn <= 5
            ORDER BY pd.operator_id, pd.metric_type, pd.metric_date DESC
        """

        params = {
            "network": network,                      # e.g., 'mainnet'
            "operator_ids": tuple(op_ids),     # tuple of ints from user input
        }

#        client.execute(query, params)

        rows = self.client.query(query, parameters=params).result_rows

        perf_data = {}
        for row in rows:
            operator_id = row[0]
            if operator_id not in perf_data:
                perf_data[operator_id] = {
                    FIELD_OPERATOR_ID: row[0],
                    FIELD_OPERATOR_NAME: row[1],
                    FIELD_IS_VO: row[2],
                    FIELD_IS_PRIVATE: row[3],
                    FIELD_VALIDATOR_COUNT: row[4],
                    FIELD_ADDRESS: row[5],
                    FIELD_PERFORMANCE_DATE: row[6],
                    FIELD_PERF_DATA_24H: {},
                    FIELD_PERF_DATA_30D: {},
                    FIELD_OPERATOR_FEE: row[9]
                }
            metric_type = row[8]
            date_str = row[6].strftime('%Y-%m-%d')
            if metric_type == '24h':
                if row[7] is not None:
                    perf_data[operator_id][FIELD_PERF_DATA_24H][date_str] = float(row[7])
            elif metric_type == '30d':
                if row[7] is not None:
                    perf_data[operator_id][FIELD_PERF_DATA_30D][date_str] = float(row[7])

        return perf_data

    # Add performance metrics to ClickHouse
    def add_performance_metric(self, network, operator_id, metric_type, metric_date, metric_value, source=None):

        query = """
            INSERT INTO performance_metrics (network, operator_id, metric_type, metric_date, metric_value)
            VALUES (%(network)s, %(operator_id)s, %(metric_type)s, %(metric_date)s, %(metric_value)s)
        """
        params = {
            'network': network,
            'operator_id': operator_id,
            'metric_type': metric_type,
            'metric_date': metric_date,
            'metric_value': metric_value,
            'source': source
        }
        
        try:
            self.client.query(query, parameters=params)
            return True
        except Exception as e:
            logging.error(f"Failed to add performance metric: {e}", exc_info=True)
            return None


    # Get the latest performance data update date from the application state
    def get_latest_perf_data_date(self, network):

        return None
    
        query = """
            SELECT state_value 
            FROM application_state 
            WHERE state_key = 'last_performance_update'
            ORDER BY updated_at DESC
            LIMIT 1
        """
        try:
            result = self.client.query(query).result_rows
            return result[0][0] if result else None
        except Exception as e:
            logging.error(f"Failed to get latest performance update date: {e}", exc_info=True)
            return None
        

    # Set the latest performance update date to the current date
    def set_latest_perf_data_date(self, network):

        return False
    
        query = """
            INSERT INTO application_state (state_key, state_value, updated_at)
            VALUES ('last_performance_update', %(current_date)s, now())
        """
        
        params = {
            'current_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        try:
            self.client.query(query, params)
            return True
        except Exception as e:
            logging.error(f"Failed to set latest performance update date: {e}", exc_info=True)
            return False

    def get_subscriptions_by_type(self, network, subscription_type):
        results = {}

        query = """
            SELECT operator_id, user_id, subscription_type
            FROM subscriptions
            WHERE subscription_type = %(subscription_type)s
            AND network = %(network)s
            AND enabled = 1
        """

        params = {
            'subscription_type': subscription_type,
            'network': network
        }

        try:
            rows = self.client.query(query, parameters=params).result_rows
            for row in rows:
                logging.debug(f"Row: {row}")
                op_id, user_id, sub_type = row
                if op_id not in results:
                    results[op_id] = {}
                # Merge if user already exists for this op_id
                if user_id not in results[op_id]:
                    results[op_id][user_id] = {}
                results[op_id][user_id][sub_type] = True
            return results

        except Exception as e:
            logging.error(f"Failed to get subscriptions by type: {e}", exc_info=True)
            return {}

        
    def get_subscriptions_by_userid(self, network, user_id):
        results = {}

        query = """
            SELECT operator_id, user_id, subscription_type 
            FROM subscriptions 
            WHERE 
                user_id = %(user_id)s AND 
                network = %(network)s AND 
                enabled = 1
        """
        params = {
            'user_id': user_id,
            'network': network
        }

        try:
            rows = self.client.query(query, parameters=params).result_rows
            for row in rows:
                logging.debug(f"Row: {row}")
                op_id, user_id, sub_type = row
                if op_id not in results:
                    results[op_id] = {}
                if user_id not in results[op_id]:
                    results[op_id][user_id] = {}
                results[op_id][user_id][sub_type] = True
            return results
        except Exception as e:
            logging.error(f"Failed to get subscriptions by user ID: {e}", exc_info=True)
            return {}



    def add_user_subscription(self, network, user_id, op_id, subscription_type):
        query = """
            INSERT INTO subscriptions (
                network, user_id, operator_id, subscription_type, enabled, updated_at
            )
            VALUES (
                %(network)s, %(user_id)s, %(operator_id)s, %(subscription_type)s, %(enabled)s, %(updated_at)s
            )
        """

        params = {
            'network': network,
            'user_id': int(user_id),
            'operator_id': op_id,
            'subscription_type': subscription_type,
            'enabled': 1,
            'updated_at': datetime.now(timezone.utc)
        }

        logging.debug(params)

        try:
            self.client.query(query, parameters=params)
            logging.info("Saved subscription.")
            return True
        except Exception as e:
            logging.error(f"Failed to add user subscription: {e}", exc_info=True)
            return None


    def del_user_subscription(self, network, user_id, op_id, subscription_type):
        query = """
            ALTER TABLE subscriptions 
            DELETE WHERE 
                network = %(network)s AND
                user_id = %(user_id)s AND 
                operator_id = %(operator_id)s AND 
                subscription_type = %(subscription_type)s
        """

        params = {
            'network': network,
            'user_id': user_id,
            'operator_id': op_id,
            'subscription_type': subscription_type
        }

        try:
            self.client.query(query, parameters=params)
            return True
        except Exception as e:
            logging.error(f"Failed to delete user subscription: {e}", exc_info=True)
            return None

