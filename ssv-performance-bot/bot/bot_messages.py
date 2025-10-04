#from bot.bot_operator_threshold_alerts import *
#from collections import defaultdict
#from datetime import datetime, timedelta
from common.config import (
    MAX_DISCORD_MESSAGE_LENGTH
#    OPERATOR_24H_HISTORY_COUNT,
#    ALERTS_THRESHOLDS_30D,
#    ALERTS_THRESHOLDS_24H,
#    FIELD_OPERATOR_REMOVED,
#    FIELD_OPERATOR_NAME,
#    FIELD_OPERATOR_ID,
#    FIELD_VALIDATOR_COUNT,
#    FIELD_PERFORMANCE,
)
#import discord
#import statistics
#import random


# Break messages into < MAX_DISCORD_MESSAGE_LENGTH characters chunks, called bundles
# Returns list of separate bundles, each < MAX_DISCORD_MESSAGE_LENGTH
# Inputs already over MAX_DISCORD_MESSAGE_LENGTH are not truncated or split
def bundle_messages(messages, max_length=MAX_DISCORD_MESSAGE_LENGTH):

    bundles = []
    cur_bundle = ''

    for message in messages:
        # Check if adding the next cur_message exceeds the limit
        if len(cur_bundle) + len(message) + 1 > max_length:  # +1 for the newline character
            bundles.append(cur_bundle)  # cur_bundle is full
            cur_bundle = message  # message we are processing becomes first in new cur_bundle
        else:
            # Room for more message. Add it to the end of cur_bundle
            cur_bundle += "\n" + message if cur_bundle else message

    # Add the cur_bundle if there's anything there
    if cur_bundle:
        bundles.append(cur_bundle)

    return bundles


# Attempts to send a direct message to a user.
# Used to notify users of problems sending direct messages to them.
async def send_direct_message_test(bot, user_id, message):
    try:
        member = await bot.fetch_user(user_id)
        await member.send(message.strip())
        return True
    except Exception as e:
        logging.error(f"Failed to send direct message test to {user_id}: {e}", exc_info=True)
        return False