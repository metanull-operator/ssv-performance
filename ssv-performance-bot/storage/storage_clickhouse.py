import time
import os
import logging
from common.config import *
from datetime import datetime, timezone, date, timedelta
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import ClickHouseError


class ClickHouseStorage:
    """
    Set a global default recency window via DATA_MAX_AGE_DAYS env or constructor.
    Override per call with the optional max_age_days param on each method.
    0 or negative => no filter (uses 1970-01-01 as the threshold).
    """

    def __init__(self, retries=5, delay=2, default_max_age_days=None, **kwargs):
        # Global default for this instance; per-call overrides are supported.
        
        env_max_age = os.environ.get("BOT_DEFAULT_MAX_AGE_DAYS", None) 
        if env_max_age is None or env_max_age != '':
            env_max_age = 0
        self.default_max_age_days = int(env_max_age if default_max_age_days is None else default_max_age_days)

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
                print(f"Attempt {attempt} failed to connect to ClickHouse: {e}")
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise  # Raise the last exception after exhausting retries

    def _updated_after(self, max_age_days: int | None) -> datetime:
        """
        Convert 'days' into an absolute timestamp for filtering updated_at.
        0 or None (when default is 0) => epoch (i.e., include everything).
        """
        days = self.default_max_age_days if max_age_days is None else int(max_age_days)
        if days <= 0:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - timedelta(days=days)

    # Get all performance data
    def get_performance_all(self, network, max_age_days: int | None = None):
        query = """
            SELECT *
            FROM performance
            WHERE network = %(network)s
              AND updated_at >= %(updated_after)s
        """
        params = {
            'network': network,
            'updated_after': self._updated_after(max_age_days),
        }
        rows = self.client.query(query, parameters=params).result_rows

        perf_data = {}
        for row in rows:
            perf_data[row[FIELD_OPERATOR_ID]] = row

        return perf_data

    def get_latest_fee_data(self, network, max_age_days: int | None = None):
        query = """
            WITH latest_counts AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, updated_at) AS validator_count
                FROM validator_counts
                WHERE network = %(network)s
                AND updated_at >= %(updated_after)s
                GROUP BY network, operator_id
            )
            SELECT 
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                lc.validator_count,         -- from validator_counts table
                o.address,
                o.operator_fee,
                o.updated_at
            FROM operators o
            LEFT JOIN latest_counts lc
            ON lc.network = o.network
            AND lc.operator_id = o.operator_id
            WHERE o.network = %(network)s
            AND o.updated_at >= %(updated_after)s
        """
        params = {
            'network': network,
            'updated_after': self._updated_after(max_age_days),
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


    def get_latest_performance_data(self, network, period, max_age_days: int | None = None):
        query = """
            WITH max_dt AS (
                SELECT max(metric_date) AS dt
                FROM performance
                WHERE network = %(network)s
                AND metric_type = %(metric_type)s
                AND updated_at >= %(updated_after)s
            ),
            latest_counts AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, updated_at) AS validator_count
                FROM validator_counts
                WHERE network = %(network)s
                AND updated_at >= %(updated_after)s
                GROUP BY network, operator_id
            )
            SELECT
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                lc.validator_count,                             -- from validator_counts table
                o.address,
                pm.metric_date,
                pm.metric_value,
                if(isNull(pm.metric_date), NULL, dateDiff('day', pm.metric_date, (SELECT dt FROM max_dt))) AS days_behind
            FROM operators o
            LEFT JOIN (
                SELECT
                    p.operator_id,
                    p.network,
                    any(p.metric_date) AS metric_date,
                    argMax(p.metric_value, (p.updated_at, p.source)) AS metric_value
                FROM performance p
                WHERE p.network     = %(network)s
                AND p.metric_type = %(metric_type)s
                AND p.metric_date = (SELECT dt FROM max_dt)
                AND p.updated_at >= %(updated_after)s
                GROUP BY p.operator_id, p.network
            ) pm
            ON pm.operator_id = o.operator_id
            AND pm.network     = o.network
            LEFT JOIN latest_counts lc
            ON lc.network = o.network
            AND lc.operator_id = o.operator_id
            WHERE o.network = %(network)s
            AND o.updated_at >= %(updated_after)s
        """

        params = {
            'metric_type': period,            # e.g., '30d'
            'network': network,               # e.g., 'mainnet'
            'updated_after': self._updated_after(max_age_days),
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

        return perf_data

    # Get performance data for specific operator IDs
    def get_performance_by_opids(self, network, op_ids, max_age_days: int | None = None):
        query = """
            WITH latest_counts AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, updated_at) AS validator_count
                FROM validator_counts
                WHERE network = %(network)s
                AND updated_at >= %(updated_after)s
                GROUP BY network, operator_id
            )
            SELECT 
                pd.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                lc.validator_count,            -- from validator_counts table (via lc)
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
                    AND updated_at >= %(updated_after)s
            ) AS pd
            LEFT JOIN operators AS o
                ON pd.network = o.network
                AND pd.operator_id = o.operator_id
                AND o.updated_at >= %(updated_after)s
            LEFT JOIN latest_counts AS lc
                ON lc.network = pd.network
                AND lc.operator_id = pd.operator_id
            WHERE pd.rn <= 5
            ORDER BY pd.operator_id ASC, pd.metric_type ASC, pd.metric_date DESC
        """

        params = {
            "network": network,                         # e.g., 'mainnet'
            "operator_ids": tuple(op_ids),              # tuple of ints from user input
            "updated_after": self._updated_after(max_age_days),
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

    # Get the latest performance data update date from the application state
    def get_latest_perf_data_date(self, network, max_age_days: int | None = None):
        query = """
            SELECT max(metric_date) AS dt
            FROM performance
            WHERE network = %(network)s
              AND updated_at >= %(updated_after)s
        """

        params = {
            'network': network,
            'updated_after': self._updated_after(max_age_days),
        }
        try:
            result = self.client.query(query, parameters=params).result_rows
            return result[0][0] if result else None
        except Exception as e:
            logging.error(f"Failed to get latest performance update date: {e}", exc_info=True)
            return None        

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
            'network': network,
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
                enabled = 1 AND
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

    def get_operators_with_validator_counts(self, network, max_age_days: int | None = None):
        query = """
        WITH latest_counts AS (
        SELECT
            network,
            operator_id,
            argMax(validator_count, updated_at) AS validator_count,
            max(updated_at)                     AS counts_latest_at
        FROM validator_counts
        WHERE network = %(network)s
        GROUP BY network, operator_id
        )
        SELECT
            o.network        AS network,
            o.operator_id    AS operator_id,
            o.operator_name  AS operator_name,
            o.is_vo          AS is_vo,
            o.is_private     AS is_private,
            IF(lc.counts_latest_at >= toDateTime(%(updated_after)s), lc.validator_count, NULL) AS validator_count
        FROM operators AS o
        LEFT JOIN latest_counts AS lc
        ON lc.network = o.network
        AND lc.operator_id = o.operator_id
        WHERE o.network = %(network)s
        AND (
                o.updated_at       >= toDateTime(%(updated_after)s)
            OR lc.counts_latest_at >= toDateTime(%(updated_after)s)
            )
        ORDER BY o.operator_id
        SETTINGS join_use_nulls = 1
        """
        res = self.client.query(
            query,
            parameters={
                "network": network,
                "updated_after": self._updated_after(max_age_days),
            },
            settings={"join_use_nulls": 1},
        )

        # Materialize to a list of dicts (never a generator)
        rows = None
        nr = getattr(res, "named_results", None)
        if callable(nr):
            try:
                rows = list(nr())
            except Exception:
                rows = None
        if not rows:
            cols = list(res.column_names)
            rows = [dict(zip(cols, r)) for r in list(res.result_rows)]

        # Optional: quick guard to catch unexpected column names
        if rows and "operator_id" not in rows[0]:
            raise KeyError(f"Expected 'operator_id' in columns, got: {list(rows[0].keys())}")

        ops = {}
        for r in rows:
            op_id = int(r["operator_id"])
            ops[op_id] = {
                FIELD_NETWORK: r["network"],
                FIELD_OPERATOR_ID: op_id,
                FIELD_OPERATOR_NAME: r["operator_name"],
                FIELD_IS_VO: int(r["is_vo"]),
                FIELD_IS_PRIVATE: int(r["is_private"]),
                # Preserve NULL from DB as Python None
                FIELD_VALIDATOR_COUNT: r["validator_count"],
            }
        return ops
