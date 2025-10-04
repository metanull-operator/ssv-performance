# Returns list of operator IDs to which the provided user ID is subscribed
# Used primarily to determine which operators to send daily DMs for
def get_user_subscriptions_by_type(subscriptions, user_id, sub_type):

    subscribed_operator_ids = []

    # Find all operator IDs to which the user ID is subscribed
    for operator_id, user_subscriptions in subscriptions.items():
        user_settings = user_subscriptions.get(user_id)
        if user_settings and user_settings.get(sub_type, False):
            subscribed_operator_ids.append(operator_id)

    return subscribed_operator_ids


# Returns list of user IDs subscribed to any of the operator IDs provided
# Used to find list of users to tag in alerts message based on all operators for which there are alerts
def get_operator_subscriptions_by_type(subscriptions, operator_ids, sub_type):

    operator_ids = list(set(operator_ids))
    subscribed_user_ids = []

    # Find all user IDs subscribed to each operator
    for op_id in operator_ids:
        if op_id in subscriptions:
            for user_id in subscriptions[op_id]:
                if sub_type in subscriptions[op_id][user_id] and subscriptions[op_id][user_id][sub_type]:
                    subscribed_user_ids.append(user_id)

    return list(set(subscribed_user_ids))
