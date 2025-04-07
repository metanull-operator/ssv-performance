import argparse
import boto3
import os
import logging
from clickhouse_connect import create_client
from datetime import datetime, timezone
from time import time

start_time = time()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_clickhouse_client():
    return create_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER"),
        password=os.environ.get("CLICKHOUSE_PASSWORD"),
        database=os.environ.get("CLICKHOUSE_DB", "default")
    )


def get_dynamodb_resource():
    return boto3.resource('dynamodb')


def migrate_operators(dynamo_table, clickhouse_client, clickhouse_table, network):
    logging.info("\n🔄 Migrating operators...")
    operators = []
    last_key = None

    while True:
        response = dynamo_table.scan(ExclusiveStartKey=last_key) if last_key else dynamo_table.scan()

        for item in response['Items']:
            operators.append((
                network,
                int(item['OperatorID']),
                item.get('Name', ''),
                int(item.get('isVO', 0)),
                int(item.get('isPrivate', 0)),
                int(item.get('ValidatorCount', 0)),
                item.get('Address', ''),
                datetime.now(timezone.utc)
            ))

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

    clickhouse_client.insert(
        table=clickhouse_table,
        data=operators,
        column_names=[
            'network',
            'operator_id',
            'operator_name',
            'is_vo',
            'is_private',
            'validator_count',
            'address',
            'updated_at'
        ]
    )
    logging.info(f"✅ Migrated {len(operators)} operators.")


def migrate_performance(dynamo_table, clickhouse_client, clickhouse_table, network, chunk_size=100):
    logging.info("\n🔄 Migrating performance data...")
    performance_data = []
    last_evaluated_key = None
    total_records = 0

    while True:
        scan_kwargs = {'Limit': chunk_size}
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = dynamo_table.scan(**scan_kwargs)
        items = response.get('Items', [])
        total_records += len(items)

        for item in items:
            operator_id = int(item['OperatorID'])

            for date, value in item.get('Performance24h', {}).items():
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                except ValueError:
                    logging.warning("Skipping malformed date '%s' for operator %s", date, operator_id)
                    continue

                performance_data.append((
                    network,
                    operator_id,
                    '24h',
                    date_obj,
                    float(value),
                    "api.ssv.network",
                    datetime.now(timezone.utc)
                ))

            for date, value in item.get('Performance30d', {}).items():
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                except ValueError:
                    logging.warning("Skipping malformed date '%s' for operator %s", date, operator_id)
                    continue

                performance_data.append((
                    network,
                    operator_id,
                    '30d',
                    date_obj,
                    float(value),
                    "api.ssv.network",
                    datetime.now(timezone.utc)
                ))

        if performance_data:
            clickhouse_client.insert(
                table=clickhouse_table,
                data=performance_data,
                column_names=[
                    "network", "operator_id", "metric_type", "metric_date",
                    "metric_value", "source", "updated_at"
                ]
            )
            performance_data.clear()

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    logging.info("✅ Performance data migration completed!")


def migrate_subscriptions(dynamo_table, clickhouse_client, clickhouse_table, network):

    logging.info("\n🔄 Migrating subscriptions...")
    response = dynamo_table.scan()
    subscriptions = [
        (network, int(item['UserID']), int(item['OperatorID']), item['SubscriptionType'])
        for item in response['Items']
    ]

    if subscriptions:
        clickhouse_client.insert(
            table=clickhouse_table,
            data=subscriptions,
            column_names=["network", "user_id", "operator_id", "subscription_type"]
        )
        logging.info(f"✅ Subscriptions migration completed! ({len(subscriptions)} records)")
    else:
        logging.error("ℹ️ No subscriptions found to migrate.")


def deduplicate_table(client, table_name: str, network: str):
    try:
        query = f"OPTIMIZE TABLE {table_name} PARTITION %(network)s FINAL"
        client.command(query, {'network': network})
        logging.info(f"✅ Deduplicated partition '{network}' in table '{table_name}'")
    except Exception as e:
        logging.error(f"❌ Failed to deduplicate {table_name}: {e}")


def verify_migration(clickhouse_client, network, table_names):
    logging.info(f"\n🔍 Verifying migration results for network: {network}")
    for table in table_names:
        try:
            count = clickhouse_client.query(
                f"SELECT count(*) FROM {table} WHERE network = %(network)s",
                {'network': network}
            ).result_rows
            logging.info(f"📊 {table}: {count[0][0]} rows")
        except Exception as e:
            logging.error(f"❌ Error verifying {table}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Migrate SSV data from DynamoDB to ClickHouse")
    parser.add_argument('--network', default=os.environ.get('NETWORK', 'mainnet'))
    parser.add_argument('--chunk-size', type=int, default=int(os.environ.get('CHUNK_SIZE', 100)))
    parser.add_argument('--dynamo-perf-table', default=os.environ.get('DYNAMO_PERF_TABLE', 'SSVPerformanceData'))
    parser.add_argument('--dynamo-sub-table', default=os.environ.get('DYNAMO_SUB_TABLE', 'SSVPerformanceSubscriptions'))
    parser.add_argument('--ch-operators-table', default=os.environ.get('CH_OPERATORS_TABLE', 'operators'))
    parser.add_argument('--ch-performance-table', default=os.environ.get('CH_PERFORMANCE_TABLE', 'performance'))
    parser.add_argument('--ch-subscriptions-table', default=os.environ.get('CH_SUBSCRIPTIONS_TABLE', 'subscriptions'))
    parser.add_argument("--log_level", default=os.environ.get("MIGRATION_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    # Set logging level dynamically
    logging.getLogger().setLevel(args.log_level.upper())
    logging.info(f"Logging level set to {args.log_level.upper()}")

    dynamodb = get_dynamodb_resource()
    clickhouse_client = get_clickhouse_client()

    ssv_perf_table = dynamodb.Table(args.dynamo_perf_table)
    ssv_sub_table = dynamodb.Table(args.dynamo_sub_table)

    migrate_operators(ssv_perf_table, clickhouse_client, args.ch_operators_table, args.network)
    migrate_performance(ssv_perf_table, clickhouse_client, args.ch_performance_table, args.network, chunk_size=args.chunk_size)

    if args.network == "mainnet":
        migrate_subscriptions(ssv_sub_table, clickhouse_client, args.ch_subscriptions_table, args.network)

    deduplicate_table(clickhouse_client, args.ch_operators_table, args.network)
    deduplicate_table(clickhouse_client, args.ch_performance_table, args.network)
    deduplicate_table(clickhouse_client, args.ch_subscriptions_table, args.network)

    verify_migration(clickhouse_client, args.network, [
        args.ch_operators_table,
        args.ch_performance_table,
        args.ch_subscriptions_table
    ])

    logging.info("🎉 Migration complete in %.2f seconds.", time() - start_time)


if __name__ == "__main__":
    main()