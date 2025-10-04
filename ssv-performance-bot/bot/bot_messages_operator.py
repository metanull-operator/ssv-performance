#from bot.bot_mentions import create_subscriber_mentions
#from bot.bot_subscriptions import get_user_subscriptions_by_type
#from bot.bot_operator_threshold_alerts import *
#from collections import defaultdict
from datetime import datetime, timedelta
from common.config import (
    OPERATOR_24H_HISTORY_COUNT,
#    ALERTS_THRESHOLDS_30D,
#    ALERTS_THRESHOLDS_24H,
#    FIELD_OPERATOR_REMOVED,
    FIELD_OPERATOR_NAME,
    FIELD_OPERATOR_ID,
    FIELD_VALIDATOR_COUNT,
#    FIELD_PERFORMANCE,
)
#import discord
#import statistics
#import random


# Create message reporting a single operator's recent performance. Overall assumption in this
# code is that the performance data for any single operator is not longer than the
# maximum Discord message length. Otherwise, each operator's message would have to be broken up.
def create_operator_performance_message(operator_data):
    message = ''

    # Display 24h performance second
    if FIELD_PERF_DATA_24H in operator_data and operator_data[FIELD_PERF_DATA_24H]:
        message += f"Recent 24h Performance:\n"

        # Get a list of dates in the last OPERATOR_24H_HISTORY_COUNT calendar days of performance data
        # Filter the performance data to data points in the last OPERATOR_24H_HISTORY_COUNT days
        # Sort the performance data by date descending
        last_x_days = [(datetime.today() - timedelta(days=x)).strftime('%Y-%m-%d') for x in range(OPERATOR_24H_HISTORY_COUNT)]
        filtered_data_points = {date: performance for date, performance in operator_data[FIELD_PERF_DATA_24H].items() if date in last_x_days}
        sorted_filtered_data_points = dict(sorted(filtered_data_points.items(), key=lambda item: item[0], reverse=True))

        if sorted_filtered_data_points:
            for data_date in sorted_filtered_data_points.keys():
                data_value = sorted_filtered_data_points[data_date]
                if data_value is not None:
                    message += f"- {data_date}: {data_value * 100:.2f}%\n"
                else:
                    message += f"- {data_date}: N/A\n"
        else:
            message += "- N/A\n"

    if FIELD_PERF_DATA_30D in operator_data and operator_data[FIELD_PERF_DATA_30D]:
        most_recent_30d_date = max(operator_data[FIELD_PERF_DATA_30D].keys())
        perf_30d = operator_data[FIELD_PERF_DATA_30D][most_recent_30d_date]

        if perf_30d:
            message += f"30d Performance: {perf_30d * 100:.2f}%\n"
        else:
            message += "30d Performance: N/A\n"
    else:
        message += "30d Performance: N/A\n"

    header = ''
    if message:
        header = f"**__{operator_data[FIELD_OPERATOR_NAME]} (ID: {operator_data[FIELD_OPERATOR_ID]}, Validators: {operator_data[FIELD_VALIDATOR_COUNT]})__**\n"

    return header + message


# Return multiple messages containing performance data for multiple
# operator IDs.
def compile_operator_performance_messages(perf_data, operator_ids):
    messages = []

    # Get the intersection of the IDs we want and the IDs in the perf_data
    reporting_ids = list(set(operator_ids) & set(perf_data.keys()))
    missing_ids = list(set(operator_ids) - set(reporting_ids))

    for operator_id in reporting_ids:
        messages.append(create_operator_performance_message(perf_data[operator_id]))

    if missing_ids and len(missing_ids) > 0:
        missing_ids_str = ', '.join(map(str, missing_ids))
        messages.append(f"Data not found for operator IDs: {missing_ids_str}")

    return messages


# Sends one or more messages detailing performance of one or more
# operator IDs, bundles messages into groups to reduce number of messages
# and ensure that messages don't exceed Discord limits.
async def send_operator_performance_messages(perf_data, ctx, operator_ids):

    op_perf_msgs = compile_operator_performance_messages(perf_data, operator_ids)

    message_bundles = bundle_messages(op_perf_msgs)
    
    responded = False
    for bundle in message_bundles:
        if not responded:
            await ctx.respond(bundle.strip(), ephemeral=False)
            responded = True
        else:
            await ctx.send_followup(bundle.strip(), ephemeral=False)