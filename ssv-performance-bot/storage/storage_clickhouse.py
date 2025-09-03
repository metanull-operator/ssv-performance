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
#    def get_performance_all(self, network, max_age_days: int | None = None):
#        query = """
#            SELECT *
#            FROM performance
#            WHERE network = %(network)s
#              AND updated_at >= %(updated_after)s
#        """
#        params = {
#            'network': network,
#            'updated_after': self._updated_after(max_age_days),
#        }
#        rows = self.client.query(query, parameters=params).result_rows
#
#        perf_data = {}
#        for row in rows:
#            perf_data[row[FIELD_OPERATOR_ID]] = row
#
#        return perf_data

    def get_latest_fee_data(self, network, max_age_days: int | None = None):
        query = """
            SELECT
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            o.operator_fee,
            lc_any.validator_count AS validator_count
            FROM operators AS o
            /* latest count (no freshness filter) to DISPLAY */
            LEFT JOIN (
            SELECT network, operator_id, validator_count, counts_latest_at
            FROM validator_counts_latest
            WHERE network = %(network)s
            ) AS lc_any
            ON lc_any.network = o.network AND lc_any.operator_id = o.operator_id
            /* fresh count (>0) to GATE inclusion for inactive operators */
            LEFT JOIN (
            SELECT network, operator_id
            FROM validator_counts_latest
            WHERE network = %(network)s
                AND counts_latest_at >= %(fresh_cutoff)s
                AND validator_count > 0
            ) AS lc_fresh
            ON lc_fresh.network = o.network AND lc_fresh.operator_id = o.operator_id
            WHERE o.network = %(network)s
            AND (
                o.updated_at >= %(fresh_cutoff)s           -- active operators
                OR lc_fresh.operator_id IS NOT NULL        -- inactive with fresh active validators
            )
            ORDER BY o.operator_id
            SETTINGS join_use_nulls = 1
        """
        params = {
            'network': network,
            "fresh_cutoff": (datetime.now(timezone.utc) - timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S"),
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
                    FIELD_OPERATOR_FEE: row[4],
                    FIELD_VALIDATOR_COUNT: row[5],
                }

        return fee_data


    def get_latest_performance_data(self, network, max_age_days: int | None = None):
        query = """
            WITH toDateTime(now('UTC') - INTERVAL 36 HOUR) AS updated_after,

            max24 AS (
                SELECT max(metric_date) AS dt
                FROM performance
                WHERE network='mainnet' AND metric_type='24h' AND updated_at >= updated_after
            ),
            max30 AS (
                SELECT max(metric_date) AS dt
                FROM performance
                WHERE network='mainnet' AND metric_type='30d' AND updated_at >= updated_after
            ),

            latest_counts AS (
                SELECT network, operator_id, validator_count, counts_latest_at
                FROM validator_counts_latest
                WHERE network='mainnet' AND counts_latest_at >= updated_after
            ),

            pm24 AS (
                SELECT
                p.network,
                p.operator_id,
                /* if you don't have p.source, use: argMax(p.metric_value, p.updated_at) */
                argMax(p.metric_value, (p.updated_at, p.source)) AS perf_24h
                FROM performance p
                WHERE p.network='mainnet'
                AND p.metric_type='24h'
                AND p.metric_date = (SELECT dt FROM max24)
                AND p.updated_at >= updated_after
                GROUP BY p.network, p.operator_id
            ),

            pm30 AS (
                SELECT
                p.network,
                p.operator_id,
                argMax(p.metric_value, (p.updated_at, p.source)) AS perf_30d
                FROM performance p
                WHERE p.network='mainnet'
                AND p.metric_type='30d'
                AND p.metric_date = (SELECT dt FROM max30)
                AND p.updated_at >= updated_after
                GROUP BY p.network, p.operator_id
            )

            SELECT
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            o.address,
            lc.validator_count,     
            pm24.perf_24h,
            pm30.perf_30d
            FROM operators o
            LEFT JOIN latest_counts lc
            ON lc.network=o.network AND lc.operator_id=o.operator_id
            LEFT JOIN pm24
            ON pm24.network=o.network AND pm24.operator_id=o.operator_id
            LEFT JOIN pm30
            ON pm30.network=o.network AND pm30.operator_id=o.operator_id
            WHERE o.network='mainnet'
            AND o.updated_at >= updated_after           
            ORDER BY o.operator_id
        """

        params = {
            'network': network,               # e.g., 'mainnet'
            "fresh_cutoff": (datetime.now(timezone.utc) - timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S"),
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
                    FIELD_ADDRESS: row[4],
                    FIELD_VALIDATOR_COUNT: row[5],
                    FIELD_PERFORMANCE: {
                        '24h': row[6],
                        '30d': row[7]
                    }
                }

        return perf_data

    # Get performance data for specific operator IDs
    def get_performance_by_opids(self, network, op_ids, max_age_days: int | None = None):
        query = """
            WITH
            toDate(%(date_from)s)   AS date_from,     -- cast to Date
            today()                 AS date_to,
            toDateTime(%(updated_after)s) AS fresh_cutoff
            , ops_in_scope AS (
            SELECT o.network, o.operator_id, o.operator_name, o.is_vo, o.is_private
            FROM operators o
            LEFT JOIN (
                SELECT network, operator_id
                FROM validator_counts_latest
                WHERE network = %(network)s
                AND counts_latest_at >= fresh_cutoff
                AND validator_count > 0
                AND operator_id IN %(operator_ids)s
            ) lc_fresh
                ON lc_fresh.network = o.network AND lc_fresh.operator_id = o.operator_id
            WHERE o.network = %(network)s
                AND o.operator_id IN %(operator_ids)s
                AND (o.updated_at >= fresh_cutoff OR lc_fresh.operator_id IS NOT NULL)
            )
            , dates AS (
            SELECT addDays(date_from, step) AS metric_date
            FROM (SELECT arrayJoin(range(toUInt32(dateDiff('day', date_from, date_to)) + 1)) AS step)
            )
            , p24 AS (
            SELECT network, operator_id, metric_date, metric_value AS perf_24h
            FROM performance_daily
            WHERE network = %(network)s AND metric_type = '24h'
                AND metric_date BETWEEN date_from AND date_to
                AND operator_id IN %(operator_ids)s
            )
            , p30 AS (
            SELECT network, operator_id, metric_date, metric_value AS perf_30d
            FROM performance_daily
            WHERE network = %(network)s AND metric_type = '30d'
                AND metric_date BETWEEN date_from AND date_to
                AND operator_id IN %(operator_ids)s
            )
            , lc_any AS (
            SELECT network, operator_id, validator_count
            FROM validator_counts_latest
            WHERE network = %(network)s AND operator_id IN %(operator_ids)s
            )
            SELECT
            o.operator_id,
            o.operator_name,
            o.is_vo,
            o.is_private,
            lc_any.validator_count,
            d.metric_date,
            p24.perf_24h,
            p30.perf_30d
            FROM ops_in_scope o
            CROSS JOIN dates d
            LEFT JOIN p24  ON p24.network = o.network AND p24.operator_id = o.operator_id AND p24.metric_date = d.metric_date
            LEFT JOIN p30  ON p30.network = o.network AND p30.operator_id = o.operator_id AND p30.metric_date = d.metric_date
            LEFT JOIN lc_any ON lc_any.network = o.network AND lc_any.operator_id = o.operator_id
            ORDER BY o.operator_id, d.metric_date
            SETTINGS join_use_nulls = 1
        """

        params = {
            "network": network,                         # e.g., 'mainnet'
            "operator_ids": tuple(op_ids) if op_ids else (0,),  # avoid IN ()
            "date_from": (date.today() - timedelta(days=7)).isoformat(),  # X-day window start
            "updated_after": (datetime.now(timezone.utc) - timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S"),
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
#                    FIELD_ADDRESS: row[5],
                    FIELD_PERFORMANCE_DATE: row[5],
                    FIELD_PERF_DATA_24H: {},
                    FIELD_PERF_DATA_30D: {},
#                    FIELD_OPERATOR_FEE: row[9]
                }
            date_str = row[5].strftime('%Y-%m-%d')
            if row[6] is not None:
                perf_data[operator_id][FIELD_PERF_DATA_24H][date_str] = float(row[6])
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

    def get_operators_with_validator_counts(self, network, max_age_days: int | None = None):
        query = """
            WITH
            toDateTime(%(updated_after)s) AS updated_after
            SELECT
            o.network        AS network,
            o.operator_id    AS operator_id,
            o.operator_name  AS operator_name,
            o.is_vo          AS is_vo,
            o.is_private     AS is_private,
            /* display count only if its latest row is fresh */
            IF(lc.counts_latest_at >= updated_after, lc.validator_count, NULL) AS validator_count
            FROM operators AS o
            LEFT JOIN (
            SELECT network, operator_id, validator_count, counts_latest_at
            FROM validator_counts_latest
            WHERE network = %(network)s
            ) AS lc
            ON lc.network = o.network
            AND lc.operator_id = o.operator_id
            WHERE o.network = %(network)s
            AND (
                    o.updated_at >= updated_after
                OR coalesce(lc.counts_latest_at, toDateTime('1970-01-01')) >= updated_after
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
