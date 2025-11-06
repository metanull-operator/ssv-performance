import time
import os
import logging
from datetime import datetime, timezone, date, timedelta
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import ClickHouseError

from common.config import *


##
## ClickHouse storage class containing methods to interact with ClickHouse database
##
class ClickHouseStorage:


    def __init__(self, retries=5, delay=2, default_max_age_days=None, **kwargs):

        # Set default max age to passed value first, then environment variable, then 0 (no limit)
        env_max_age = os.environ.get("BOT_DEFAULT_MAX_AGE_DAYS", None) 
        if env_max_age is None or env_max_age != '':
            env_max_age = 0
        self.default_max_age_days = int(env_max_age if default_max_age_days is None else default_max_age_days)

        # Multiple connection attempts to ClickHouse database
        for attempt in range(1, retries + 1):
            try:
                self.client = create_client(
                    host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
                    port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
                    username=os.environ.get("CLICKHOUSE_USER", "ssv_performance"),
                    password=kwargs.get("password"),
                    database=os.environ.get("CLICKHOUSE_DB", "default")
                )
                break
            except ClickHouseError as e:
                print(f"Attempt {attempt} failed to connect to ClickHouse: {e}")
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise


    ##
    ## Build a date for filtering the updated_at field based on max_age_days.
    ## With None or 0, return epoch to return everything.
    ##
    def _updated_after(self, max_age_days: int | None) -> datetime:
        days = self.default_max_age_days if max_age_days is None else int(max_age_days)
        if days <= 0:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - timedelta(days=days)


    def get_latest_fee_data(self, network, max_age_days: int | None = None):

        query = """
            WITH toDateTime(now('UTC') - toIntervalHour(36)) AS metric_after,

            o AS (
                SELECT
                    network,
                    operator_id,
                    operator_name,
                    is_vo,
                    is_private,
                    updated_at
                FROM operators
                WHERE network = %(network)s
            ),

            lc_raw AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, (metric_date, updated_at)) AS validator_count,
                    argMax(metric_date, (metric_date, updated_at)) AS validator_count_metric_date
                    FROM validator_counts
                WHERE
                    network = %(network)s
                GROUP BY network, operator_id
            ),

            lc AS (
                SELECT
                    *,
                    (validator_count_metric_date >= metric_after AND validator_count > 0) AS is_count_fresh
                FROM lc_raw
            ),

            fee_fresh AS (
                SELECT
                    network,
                    operator_id,
                    argMax(operator_fee, (metric_date, updated_at)) AS operator_fee,
                    argMax(metric_date, (metric_date, updated_at)) AS operator_fee_metric_date
                FROM operator_fees
                WHERE 
                    network = %(network)s
                    AND metric_date >= metric_after
                GROUP BY network, operator_id
            )

            SELECT
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                fee_fresh.operator_fee,
                lc.validator_count,
                lc.is_count_fresh,
                fee_fresh.operator_fee_metric_date,
            FROM o
            INNER JOIN fee_fresh
                ON fee_fresh.network = o.network
                AND fee_fresh.operator_id = o.operator_id
            LEFT JOIN lc
                ON lc.network = o.network
                AND lc.operator_id = o.operator_id
            WHERE (o.updated_at >= metric_after) OR coalesce(lc.is_count_fresh, 0)
            ORDER BY o.operator_id
            SETTINGS join_use_nulls = 1
        """
        params = {
            'network': network,
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
            WITH toDateTime(now('UTC') - toIntervalHour(36)) AS metric_after,

            max24 AS (
                SELECT
                    max(metric_date) AS dt
                FROM performance
                WHERE
                    network = %(network)s
                    AND metric_type = '24h'
                    AND metric_date >= metric_after
            ),

            max30 AS (
                SELECT
                    max(metric_date) AS dt
                FROM performance
                WHERE
                    network = %(network)s
                    AND metric_type = '30d'
                    AND metric_date >= metric_after
            ),

            latest_counts AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, (metric_date, updated_at)) AS validator_count,
                    argMax(updated_at, (metric_date, updated_at)) AS counts_latest_at
                FROM validator_counts
                WHERE network = %(network)s
                GROUP BY network, operator_id
            ),

            pm24 AS (
                SELECT
                    p.network,
                    p.operator_id,
                    argMax(p.metric_value, p.updated_at) AS perf_24h
                FROM performance p
                WHERE
                    p.network = %(network)s
                    AND p.metric_type = '24h'
                    AND p.metric_date = (SELECT dt FROM max24)
                GROUP BY p.network, p.operator_id
            ),

            pm30 AS (
                SELECT
                    p.network,
                    p.operator_id,
                    argMax(p.metric_value, p.updated_at) AS perf_30d
                FROM performance p
                WHERE
                    p.network = %(network)s
                    AND p.metric_type = '30d'
                    AND p.metric_date = (SELECT dt FROM max30)
                GROUP BY p.network, p.operator_id
            )

            SELECT
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                o.address,
                IF(lc.counts_latest_at >= metric_after, lc.validator_count, NULL) AS validator_count,
                lc.counts_latest_at,
                pm24.perf_24h,
                pm30.perf_30d,
                o.updated_at
            FROM operators AS o
            LEFT JOIN latest_counts AS lc
                ON lc.network = o.network
                AND lc.operator_id = o.operator_id
            LEFT JOIN pm24
                ON pm24.network = o.network
                AND pm24.operator_id = o.operator_id
            LEFT JOIN pm30
                ON pm30.network = o.network
                AND pm30.operator_id = o.operator_id
            WHERE o.network = %(network)s
            ORDER BY o.operator_id
            SETTINGS join_use_nulls = 1
        """

        params = {
            'network': network,
        }

        rows = self.client.query(query, parameters=params).result_rows

        fresh_cutoff = datetime.now(timezone.utc) - timedelta(hours=36)

        perf_data = {}
        for row in rows:
            operator_id = row[0]
            if operator_id not in perf_data:
                updated_at = row[9]
                if updated_at is not None and getattr(updated_at, "tzinfo", None) is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)

                validator_count     = row[5]
                counts_latest_at    = row[6]     # ← now available
                if counts_latest_at is not None and getattr(counts_latest_at, "tzinfo", None) is None:
                    counts_latest_at = counts_latest_at.replace(tzinfo=timezone.utc)

                # fresh & active count?
                has_fresh_active = (
                    validator_count is not None
                    and int(validator_count) > 0
                    and counts_latest_at is not None
                    and counts_latest_at >= fresh_cutoff
                )

                # “removed” = stale operator record, but fresh active validators
                is_removed = (updated_at is None or updated_at < fresh_cutoff) and has_fresh_active

                perf_data[operator_id] = {
                    FIELD_OPERATOR_ID: row[0],
                    FIELD_OPERATOR_NAME: row[1],
                    FIELD_IS_VO: row[2],
                    FIELD_IS_PRIVATE: row[3],
                    FIELD_ADDRESS: row[4],
                    FIELD_VALIDATOR_COUNT: row[5],           # already nulled if stale in SQL
                    FIELD_VALIDATOR_COUNTS_LATEST_AT: counts_latest_at,
                    FIELD_PERFORMANCE: {'24h': row[7], '30d': row[8]},
                    FIELD_OPERATOR_UPDATED_AT: updated_at,
                    FIELD_OPERATOR_REMOVED: is_removed,
                }

                if (operator_id == 14):
                    logging.debug(f"Debug: Operator ID 14 data: {perf_data[operator_id]}")

        return perf_data


    # Get performance data for specific operator IDs
    def get_performance_by_opids(self, network, op_ids, max_age_days: int | None = None):

        op_ids = [int(x) for x in (op_ids or []) if x is not None]
        if not op_ids:
            op_ids = [0]

        if max_age_days is None or max_age_days < 0:
            max_age_days = 7
        max_age_days = int(max_age_days)

        query = """
            WITH
                toDate(now('UTC') - toIntervalDay(%(max_age)s)) AS date_from,
                toDate(now('UTC'))                              AS date_to,
                toDateTime(%(updated_after)s)                   AS metric_after,

            lc_raw AS (
                SELECT
                    network,
                    operator_id,
                    argMax(validator_count, (metric_date, updated_at)) AS validator_count,
                    argMax(metric_date, (metric_date, updated_at)) AS metric_date_chosen,
                    argMax(updated_at, (metric_date, updated_at)) AS validator_count_updated_at
                FROM validator_counts
                WHERE
                    network = %(network)s
                    AND operator_id IN %(operator_ids)s
                GROUP BY network, operator_id
            ),

            lc AS (
                SELECT
                    *,
                    (metric_date_chosen >= metric_after AND validator_count > 0) AS is_count_fresh
                FROM lc_raw
            ),

            ops_in_scope AS (
                SELECT
                    o.network,
                    o.operator_id,
                    o.operator_name,
                    o.is_vo,
                    o.is_private,
                    lc.validator_count AS validator_count,
                    lc.validator_count_updated_at AS validator_count_updated_at,
                    (lc.metric_date_chosen >= metric_after AND validator_count > 0) AS validator_count_is_fresh
                FROM operators o
                LEFT JOIN lc
                    ON lc.network = o.network
                    AND lc.operator_id = o.operator_id
                WHERE
                    o.network = %(network)s
                    AND o.operator_id IN %(operator_ids)s
                    AND (o.updated_at >= metric_after OR coalesce(lc.is_count_fresh, 0))
            ),

            dates AS (
                WITH toUInt64(greatest(dateDiff('day', date_from, date_to), 0)) AS days_span
                SELECT addDays(date_from, number) AS metric_date
                FROM numbers(days_span + 1)
            ),

            p24 AS (
                SELECT
                    network,
                    operator_id,
                    metric_date,
                    argMax(metric_value, last_row_at) AS perf_24h
                FROM performance_daily
                WHERE
                    network = %(network)s
                    AND metric_type = '24h'
                    AND metric_date BETWEEN date_from AND date_to
                    AND operator_id IN %(operator_ids)s
                GROUP BY network, operator_id, metric_date
            ),

            p30 AS (
                SELECT 
                    network,
                    operator_id,
                    metric_date,
                    argMax(metric_value, last_row_at) AS perf_30d
                FROM performance_daily
                WHERE
                    network = %(network)s
                    AND metric_type = '30d'
                    AND metric_date BETWEEN date_from AND date_to
                    AND operator_id IN %(operator_ids)s
                GROUP BY network, operator_id, metric_date
            )

            SELECT
                o.operator_id,
                o.operator_name,
                o.is_vo,
                o.is_private,
                o.validator_count,
                o.validator_count_updated_at,
                d.metric_date,
                p24.perf_24h,
                p30.perf_30d,
                o.validator_count_is_fresh
            FROM ops_in_scope o
            CROSS JOIN dates d
            LEFT JOIN p24
                ON p24.network = o.network
                AND p24.operator_id = o.operator_id
                AND p24.metric_date = d.metric_date
            LEFT JOIN p30
                ON p30.network = o.network
                AND p30.operator_id = o.operator_id
                AND p30.metric_date = d.metric_date
            ORDER BY o.operator_id, d.metric_date
            SETTINGS join_use_nulls = 1
        """

        params = {
            'network': str(network),
            'operator_ids': [int(x) for x in op_ids],  # ensure pure built-in ints
            'max_age': max_age_days,
            'updated_after': (datetime.now(timezone.utc) - timedelta(hours=36)).strftime('%Y-%m-%d %H:%M:%S'),
        }

        logging.debug(f"Query params: {params}")

        rows = self.client.query(query, parameters=params).result_rows

        logging.debug(f"Fetched {len(rows)} performance rows for operators: {op_ids}")

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
                    FIELD_VALIDATOR_COUNTS_LATEST_AT: row[5],
#                    FIELD_ADDRESS: row[5],
                    FIELD_PERFORMANCE_DATE: row[6],
                    FIELD_PERF_DATA_24H: {},
                    FIELD_PERF_DATA_30D: {},
#                    FIELD_OPERATOR_FEE: row[9]
                }

            date_str = row[6].strftime('%Y-%m-%d')
            if row[7] is not None:
                perf_data[operator_id][FIELD_PERF_DATA_24H][date_str] = float(row[7])
            if row[8] is not None:
                perf_data[operator_id][FIELD_PERF_DATA_30D][date_str] = float(row[8])

        return perf_data


    # Get the latest performance data update date from the application state
    def get_latest_perf_data_date(self, network, max_age_days: int | None = None):
        query = """
            SELECT max(metric_date) AS dt
            FROM performance
            WHERE network = %(network)s
        """

        params = {
            'network': network
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
                toDateTime(%(updated_after)s) AS updated_after,
                %(network)s                   AS network_param,

                lc AS (
                    SELECT
                        network,
                        operator_id,
                        argMax(validator_count, updated_at) AS validator_count,
                        max(updated_at)                     AS counts_latest_at
                    FROM validator_counts
                    WHERE network = network_param
                    GROUP BY network, operator_id
                )

            SELECT
                o.network       AS network,
                o.operator_id   AS operator_id,
                o.operator_name AS operator_name,
                o.is_vo         AS is_vo,
                o.is_private    AS is_private,
                IF(lc.counts_latest_at >= updated_after /* AND lc.validator_count > 0 */, lc.validator_count, NULL) AS validator_count
            FROM operators AS o
            LEFT JOIN lc
                ON lc.network = o.network
            AND lc.operator_id = o.operator_id
            WHERE
                o.network = network_param
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
