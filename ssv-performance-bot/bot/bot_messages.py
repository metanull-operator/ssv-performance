import logging
from common.config import *


##
## Take a list of messages and concatenate them into < MAX_DISCORD_MESSAGE_LENGTH
## character chunks. Return list of message chunks.
## Inputs already over MAX_DISCORD_MESSAGE_LENGTH are not modified.
##
def bundle_messages(messages, max_length=MAX_DISCORD_MESSAGE_LENGTH):

    bundles = []
    cur_bundle = ''

    for message in messages:
        # Check if adding the next cur_message exceeds the limit
        if len(cur_bundle) + len(message) + 1 > max_length:  # +1 for the newline character
            bundles.append(cur_bundle)  # Over length, so save the current bundle and start a new one
            cur_bundle = message  # Current message becomes first in new bundle
        else:
            # Room for more message. Add it to the end of the current bundle
            cur_bundle += "\n" + message if cur_bundle else message

    # Add the cur_bundle if there's anything there
    if cur_bundle:
        bundles.append(cur_bundle)

    return bundles

##
## Attempts to send a direct message to a user.
## Used to notify users of problems sending direct messages to them.
##
async def send_direct_message_test(bot, user_id, message):
    try:
        member = await bot.fetch_user(user_id)
        await member.send(message.strip())
        return True
    except Exception as e:
        logging.error(f"Failed to send direct message test to {user_id}: {e}", exc_info=True)
        return False