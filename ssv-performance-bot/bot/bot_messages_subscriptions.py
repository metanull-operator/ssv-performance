from bot.bot_subscriptions import get_user_subscriptions_by_type


##
## Creates a message listing subscriptions for a particular user.
##
def create_subscriptions_message(user_data, subscriber):

    sub_daily = get_user_subscriptions_by_type(user_data, subscriber.id, 'daily')
    sub_alerts = get_user_subscriptions_by_type(user_data, subscriber.id, 'alerts')

    message = f"**__Operator ID Subscriptions:__**\n"

    # List daily direct message subscriptions
    if sub_daily and len(sub_daily) > 0:
        message += f"- Daily performance direct messages: {', '.join(map(str, sub_daily))}\n"
    else:
        message += f"- Daily performance direct messages: None\n"

    # List VO performance threshold subscriptions
    if sub_alerts and len(sub_alerts) > 0:
        message += f"- VOC performance threshold @mentions: {', '.join(map(str, sub_alerts))}\n"
    else:
        message += f"- VOC performance threshold @mentions: None\n"

    return message