#from bot.bot_mentions import create_subscriber_mentions
#from bot.bot_subscriptions import get_user_subscriptions_by_type
from bot.bot_operator_threshold_alerts import *
#from collections import defaultdict
#from datetime import datetime, timedelta
from common.config import (
#    OPERATOR_24H_HISTORY_COUNT,
#    ALERTS_THRESHOLDS_30D,
#    ALERTS_THRESHOLDS_24H,
#    FIELD_OPERATOR_REMOVED,
    FIELD_OPERATOR_NAME,
    FIELD_OPERATOR_ID,
    FIELD_VALIDATOR_COUNT,
#    FIELD_PERFORMANCE,
)
import discord
#import statistics
#import random


# Creates a message bullet item for a single period performance data point
def get_latest_performance(period, operator, attribute):

    try:
        if not operator.get(attribute):
            logging.error(f"{period} performance data attribute not present in get_latest_performance()")
            return f"- {period}: {period} performance data is not available\n"

        most_recent_date = max(operator[attribute].keys())
        most_recent_performance = operator[attribute][most_recent_date]

        return f"- {period}: {most_recent_performance * 100:.2f}%\n" if most_recent_performance else f"- {period}: {period} performance data is not available\n"
    except Exception as e:
        logging.error(f"Exception in get_latest_performance(): {e}", exc_info=True)
        return f"- {period}: {period} performance data is not available\n"


# Create a performance message for a single operator
def create_daily_operator_message(operator):
    message = f"\n**__{operator[FIELD_OPERATOR_NAME]} (ID: {operator[FIELD_OPERATOR_ID]}, Validators: {operator[FIELD_VALIDATOR_COUNT]}):__**\n"

    message += get_latest_performance("24h", operator, FIELD_PERF_DATA_24H)
    message += get_latest_performance("30d", operator, FIELD_PERF_DATA_30D)

    return message


# Create a dict of daily performance messages to send to Discord users
# Loops through subscriptions for each operator ID and appends the
# operator performance data to a dict of messages to go to each user
def compile_daily_operator_messages(perf_data, subscriptions):
    user_messages = {}

    # Looping through all subscribed users for each operator
    for op_id, users in subscriptions.items():
        op_id = int(op_id)

        # Create the direct message text if there is performance data
        if op_id in perf_data:
            op_performance_message = create_daily_operator_message(perf_data[op_id])

            # Find all the daily subscriptions to that operator ID and
            # add to the list of messages for that user
            for user, notification_types in users.items():
                if notification_types.get("daily", False):
                    if user not in user_messages:
                        user_messages[user] = []
                    user_messages[user].append(op_performance_message)

    return user_messages


# Gets dict of all messages going out to all users and sends them,
# breaking messages into chunks less than maximum message length for Discord.
async def send_daily_direct_messages(bot, perf_data, subscriptions, dm_recipients=[]):
    user_messages = compile_daily_operator_messages(perf_data, subscriptions)

    # Send out the compiled messages to each user
    for user, messages in user_messages.items():
        try:
            member = await bot.fetch_user(user)
        except Exception as e:
            logging.error(f"Unable to fetch user {user} in send_daily_direct_messages(): {e}", exc_info=True)
            continue

        if member:
            if dm_recipients and member.id not in dm_recipients:
                continue

            bundles = bundle_messages(messages)
            for bundle in bundles:
                message = bundle.strip()
                if message:
                    try:
                        await member.send(bundle.strip())
                    except discord.Forbidden as e:
                        if e.code == 50007:
                            logging.warning(f"User {member.name}#{member.discriminator}/{member.display_name}/{user} has DMs disabled. Skipping DM.")
                        else:
                            logging.error(f"Forbidden error sending DM to {user}: {e}", exc_info=True)
                    except Exception as e:
                        logging.error(f"Unexpected error sending DM to {user}: {e}", exc_info=True)
