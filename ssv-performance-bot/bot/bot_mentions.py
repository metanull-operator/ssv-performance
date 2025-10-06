from common.config import *
from bot.bot_subscriptions import get_operator_subscriptions_by_type


##
## Query for guild member by user_id and return mention text
##
def mention_member(guild, user_id):
    member = guild.get_member(int(user_id))
    if member:
        return f"{member.mention} "

    return ''


## 
## Create mention messages for users subscribed to any of the provided operator IDs
##
def create_subscriber_mentions(guild, subscriptions, operator_ids, notification_type, dm_recipients=[]):
    messages = []

    user_ids = get_operator_subscriptions_by_type(subscriptions, operator_ids, notification_type)

    mention_msg = "\n"
    for user_id in user_ids:

        # If dm_recipients is provided, only include those users. For QA/testing,
        # to filter out users that may have been added to subscriptions as a test,
        # or when production databases have been migrated back to development/staging
        # environments.
        if dm_recipients and user_id not in dm_recipients:
            continue

        mention = mention_member(guild, user_id)

        if mention:
            if len(mention_msg) + len(mention) + 1 > MAX_DISCORD_MESSAGE_LENGTH:  # +1 for whitespace
                messages.append(mention_msg)
                mention_msg = "\n" + mention
            else:
                mention_msg += ' ' + mention

    # Flush any remaining message text
    if mention_msg:
        messages.append(mention_msg)

    return messages