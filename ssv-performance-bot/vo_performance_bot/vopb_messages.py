from vo_performance_bot.vopb_mentions import create_subscriber_mentions
from vo_performance_bot.vopb_subscriptions import get_user_subscriptions_by_type
from vo_performance_bot.vopb_operator_threshold_alerts import *
from datetime import datetime, timedelta
from common.config import OPERATOR_24H_HISTORY_COUNT, ALERTS_THRESHOLDS_30D, ALERTS_THRESHOLDS_24H
import discord
import statistics
import random


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


# Creates a message listing subscriptions for a particular user.
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


def create_alerts_24h(perf_data):
    alert_msgs_24h = {threshold: [] for threshold in ALERTS_THRESHOLDS_24H}
    operator_ids = []

    logging.debug(f"Creating 24h alerts for {len(perf_data)} operators")

    for op_id in perf_data.keys():
        operator = perf_data[op_id]

        logging.debug(f"Creating 24h alerts for operator {op_id}")

        if not operator[FIELD_IS_VO]:
            logging.debug(f"Operator {op_id} is not a VO")
            continue

        validator_count = operator[FIELD_VALIDATOR_COUNT]
        if validator_count is None or int(validator_count) <= 0:
            logging.debug(f"Operator {op_id} has no validators")
            continue

        logging.debug(f"{alert_msgs_24h}")

        for threshold, alert_list in alert_msgs_24h.items():
            logging.debug(f"Checking operator {op_id} against threshold {threshold}")
            result = operator_threshold_alert_24h(operator, threshold)
            if result and validator_count > 0:
                operator_ids.append(result[FIELD_OPERATOR_ID])
                performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_str}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count})"
                alert_list.append(alert)

    return operator_ids, alert_msgs_24h


def create_alerts_30d(perf_data):
    alert_msgs_30d = {threshold: [] for threshold in ALERTS_THRESHOLDS_30D}
    operator_ids = []

    for op_id in perf_data.keys():
        operator = perf_data[op_id]

        if not operator[FIELD_IS_VO]:
            continue

        validator_count = operator[FIELD_VALIDATOR_COUNT]
        if validator_count is None or int(validator_count) <= 0:
            continue

        for threshold, alert_list in alert_msgs_30d.items():
            result = operator_threshold_alert_30d(operator, threshold)
            if result and validator_count > 0:
                operator_ids.append(result[FIELD_OPERATOR_ID])
                performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_str}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count})"
                alert_list.append(alert)

    return operator_ids, alert_msgs_30d


def compile_alert_threshold_groups(alerts, period_label):
    messages = []

    for threshold, alert_list in alerts.items():
        title = f"\n**__{period_label} < {threshold:.0%}:__**\n"
        message_bundles = bundle_messages(alert_list, MAX_DISCORD_MESSAGE_LENGTH - len(title))

        for bundle in message_bundles:
            messages.append(title + bundle)

    return messages


# Compile alerts, mentions and any extra message into a single set of separate messages to be sent to Discord.
# This attempts to push everything into as few messages as possible to not bomb Discord with excessive messages.
def compile_vo_threshold_messages(perf_data, extra_message=None, subscriptions=None, guild=None, dm_recipients=[], mention_periods=[]):

    # Get alerts for different time periods
    operator_ids_24h, alerts_24h = create_alerts_24h(perf_data)
    operator_ids_30d, alerts_30d = create_alerts_30d(perf_data)

    messages = []

    mentions_24h = []
    messages.extend(compile_alert_threshold_groups(alerts_24h, "24h"))
    if subscriptions and guild and '24h' in mention_periods:
        mentions_24h = create_subscriber_mentions(guild, subscriptions, operator_ids_24h, 'alerts', dm_recipients)

    mentions_30d = []
    messages.extend(compile_alert_threshold_groups(alerts_30d, "30d"))
    if subscriptions and guild and '30d' in mention_periods:
        mentions_30d = create_subscriber_mentions(guild, subscriptions, operator_ids_30d, 'alerts', dm_recipients)

    mentions = mentions_24h + mentions_30d
    mentions = list(dict.fromkeys(mentions))

    messages.extend(mentions)

    # Include an extra message, if configured
    if extra_message and len(extra_message) > 0:
        if mentions:
            messages.append("\n" + extra_message)
        else:
            messages.append(extra_message)

    # Rebundle everything up again to reduce down to the fewest messages to post to Discord
    bundles = bundle_messages(messages)

    return(bundles)


async def send_vo_threshold_messages(channel, perf_data, extra_message=None, subscriptions=None, dm_recipients=[], mention_periods=[]):

    try:
        # Only attempt @mentions if we have a guild to query and subscription info
        if channel and hasattr(channel, 'guild') and subscriptions:
            messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message, subscriptions=subscriptions, guild=channel.guild, dm_recipients=dm_recipients, mention_periods=mention_periods)
        else:
            messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message, dm_recipients=dm_recipients)

        if messages:
            for message in messages:
                await channel.send(message.strip())
        else:
            current_date = datetime.now().strftime("%Y-%m-%d")
            await channel.send(f'No performance alerts for {current_date}.')
    except Exception as e:
        logging.error(f"Failed to send VO threshold messages: {e}", exc_info=True)


def iqr_bucket_lines_with_zero_handling(values, fees, num_buckets=5, iqr_multiplier=1.5):
    zero_fees = [(fee, op) for fee, op in fees if fee == 0]
    non_zero_fees = [(fee, op) for fee, op in fees if fee > 0]

    if not non_zero_fees:
        return [], [], len(zero_fees), []

    non_zero_values = [fee for fee, _ in non_zero_fees]

    if len(non_zero_values) < 2:
        # fallback only includes non-zero fees in the bucket
        fees_only = [fee for fee, _ in non_zero_fees]
        min_fee = min(fees_only)
        max_fee = max(fees_only)
        
        return [non_zero_fees], [(min_fee, max_fee)], len(zero_fees), []

    # IQR-based outlier detection
    q1 = statistics.quantiles(non_zero_values, n=4)[0]
    q3 = statistics.quantiles(non_zero_values, n=4)[2]
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * iqr

    inlier_fees = [(fee, op) for fee, op in non_zero_fees if 0 < fee <= upper_bound]
    outlier_fees = [(fee, op) for fee, op in non_zero_fees if fee > upper_bound]

    if zero_fees:
        min_val = min(fee for fee, _ in inlier_fees)
    else:
        min_val = min(fee for fee, _ in inlier_fees + outlier_fees)

    max_val = max(fee for fee, _ in inlier_fees)
    bucket_size = (max_val - min_val) / (num_buckets or 1)

    # Build buckets and ranges
    buckets = [[] for _ in range(num_buckets)]
    bucket_ranges = []

    for i in range(num_buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        bucket_ranges.append((lower, upper))

    for fee, op in inlier_fees:
        if fee == 0:
            continue  
        i = int((fee - min_val) / bucket_size)
        if i == num_buckets:
            i -= 1
        buckets[i].append((fee, op))

    assert len(zero_fees) + sum(len(b) for b in buckets) + len(outlier_fees) == len(fees), \
        "Mismatch in total operator counts"

    return buckets, bucket_ranges, len(zero_fees), outlier_fees


def render_bucket_lines(buckets_with_ranges, zero_count, outliers, fees, mean, median, max_segments=20):

    def validator_sum(entries):
        return sum(op.get(FIELD_VALIDATOR_COUNT, 0) for _, op in entries)

    max_count = max([len(b) for b, _, _ in buckets_with_ranges] + [zero_count, len(outliers)])
    lines = []

    def build_bar(count):
        if count == 0:
            return ""
        return "■" * max(1, int((count / max_count) * max_segments))

    all_counts = [zero_count] + [len(b) for b, _, _ in buckets_with_ranges] + [len(outliers)]
    count_width = max(len(str(c)) for c in all_counts) + 2

    labels = ["0.00"]  # zero bucket label

    # Add range labels
    for _, lower, upper in buckets_with_ranges:
        labels.append(f"{lower:.2f}–{upper:.2f}")

    # Add outlier label if any
    if outliers:
        outlier_min = min(fee for fee, _ in outliers)
        labels.append(f">= {outlier_min:.2f}")

    label_width = max(len(label) for label in labels) + 1

    if zero_count > 0:
        bar = build_bar(zero_count)

        markers = []
        if mean is not None and mean == 0:
            markers.append("mean")
        if median is not None and median == 0:
            markers.append("median")

        marker_str = f"⟵ {', '.join(markers)}" if markers else ""

        validator_count = sum(op[FIELD_VALIDATOR_COUNT] for fee, op in fees if fee == 0)
        count_str = f"({zero_count})"

        lines.append(f"{'0.00':>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {marker_str}")


    for b, lower, upper in buckets_with_ranges:
        label = f"{lower:.2f}–{upper:.2f}"
        b_len = len(b)
        bar = build_bar(b_len)

        markers = []
        if mean is not None and mean != 0 and lower <= mean <= upper:
            markers.append("mean")
        if median is not None and median != 0 and lower <= median <= upper:
            markers.append("median")

        marker_str = f"⟵ {', '.join(markers)}" if markers else ""

        validator_count = sum(op[FIELD_VALIDATOR_COUNT] for fee, op in b)
        count_str = f"({b_len})"

        lines.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {marker_str}")

    if outliers:
        count = len(outliers)
        outlier_min = min(fee for fee, _ in outliers)
        outlier_max = max(fee for fee, _ in outliers)
        bar = build_bar(count)
        validator_count = sum(op[FIELD_VALIDATOR_COUNT] for fee, op in outliers)
        count_str = f"({count})"
        label = f">= {outlier_min:.2f}"

        single_plural = '' if count == 1 else 's'

        if outlier_min == outlier_max:
            outlier_info = f"⟵ outlier{single_plural}: {outlier_min:.2f}"
        else:
            outlier_info = f"⟵ outliers: {outlier_min:.2f}-{outlier_max:.2f}"

        lines.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {outlier_info}")

    lines = ["```"] + lines + ["```"]

    return bundle_messages(lines)


def compile_fee_messages(fee_data, extra_message=None, availability="public", verified="all", num_segments=20):
    messages = []

    public_fees = []
    private_fees = []

    public_vo_fees = []
    public_non_vo_fees = []
    private_vo_fees = []
    private_non_vo_fees = []

    all_fees = []

    for operator in fee_data.values():
        fee = operator.get(FIELD_OPERATOR_FEE)
        is_private = operator.get(FIELD_IS_PRIVATE)
        is_vo = operator.get(FIELD_IS_VO)
        if fee is None:
            continue

        item = (fee, operator)

        all_fees.append(item)

        if is_private:
            private_fees.append(item)
            if is_vo:
                private_vo_fees.append(item)
            else:
                private_non_vo_fees.append(item)
        else:
            public_fees.append(item)
            if is_vo:
                public_vo_fees.append(item)
            else:
                public_non_vo_fees.append(item)


    def summarize(label, fees, num_buckets=5, iqr_multiplier=1.5, num_segments=20):
        if not fees:
            return [f"No {label} operators found."]

        values = [f[0] for f in fees]
        sorted_fees = sorted(fees, key=lambda x: x[0])
        highest = sorted_fees[-1]
        lowest = sorted_fees[0]
        count = len(values)

        # Get structured buckets
        buckets, bucket_ranges, zero_count, outliers = iqr_bucket_lines_with_zero_handling(
            values, fees, num_buckets=num_buckets, iqr_multiplier=iqr_multiplier
        )

        mean = statistics.mean(values)
        median = statistics.median(values)        

        # Render aligned bar lines
        bucket_lines = render_bucket_lines(
            buckets_with_ranges=[(bucket, lower, upper) for bucket, (lower, upper) in zip(buckets, bucket_ranges)],
            zero_count=zero_count,
            outliers=outliers,
            fees=fees,
            max_segments=num_segments,
            mean=mean,
            median=median
        )

        lines = [
            f"**{label} Operators (SSV/year)**",
            f"*{count} operators*",
            f"- Mean Fee: {mean:.2f}",
            f"- Median Fee: {median:.2f}",
        ]

        lowest_fee = lowest[0]
        lowest_operators = [op for fee, op in fees if fee == lowest_fee]

        if len(lowest_operators) == 1:
            op = lowest_operators[0]
            lines.append(
                f"- Lowest Fee: {lowest_fee:.2f} - {op[FIELD_OPERATOR_NAME]} (ID: {op[FIELD_OPERATOR_ID]}, Validators: {op[FIELD_VALIDATOR_COUNT]})"
            )
        else:
            example_op = random.choice(lowest_operators)
            lines.append(f"- Lowest Fee: {lowest_fee:.2f} - {example_op[FIELD_OPERATOR_NAME]} (ID: {example_op[FIELD_OPERATOR_ID]}, Validators: {example_op[FIELD_VALIDATOR_COUNT]}) and {len(lowest_operators)-1} other operator(s)")

        lines.append(
            f"- Highest Fee: {highest[0]:.2f} - {highest[1][FIELD_OPERATOR_NAME]} "
            f"(ID: {highest[1][FIELD_OPERATOR_ID]}, Validators: {highest[1][FIELD_VALIDATOR_COUNT]})"
        )

        lines.append(f"### {label} Operator Fee Distribution (Operators)")
        lines += bucket_lines

        return bundle_messages(lines)

    if availability == "all" and verified == "all":
        messages.extend(summarize("All", all_fees, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))

    # Public breakdown
    if availability in ("public"):
        if verified == "all":
            messages.extend(summarize("All Public", public_fees, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified in ("all", "verified"):
            messages.extend(summarize("Public Verified", public_vo_fees, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified in ("all", "unverified"):
            messages.extend(summarize("Public Unverified", public_non_vo_fees, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
 
    # Private breakdown
    if availability in ("private"):
        if verified == "all":
            messages.extend(summarize("All Private", private_fees, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))
        if verified in ("all", "verified"):
            messages.extend(summarize("Private Verified", private_vo_fees, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))
        if verified in ("all", "unverified"):
            messages.extend(summarize("Private Unverified", private_non_vo_fees, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))

    if extra_message:
        messages.append(extra_message)

    return bundle_messages(messages)


async def respond_fee_messages(ctx, fee_data, extra_message=None, availability="public", verified="all", num_segments=20):
    try:
        messages = compile_fee_messages(fee_data, extra_message=extra_message, availability=availability, verified=verified, num_segments=num_segments)

        if messages:
            for message in messages:
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            await ctx.followup.send("Fee data not found.", ephemeral=True)
    except Exception as e:
        logging.error(f"Failed to respond with fee data message: {e}", exc_info=True)


async def respond_vo_threshold_messages(ctx, perf_data, extra_message=None):

    try:
        messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message)

        if messages:
            for message in messages:
                # Note assumption that defer() was previously called.
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            current_date = datetime.now().strftime("%Y-%m-%d")
            await ctx.followup.send(f'No performance alerts for {current_date}.', ephemeral=False)
    except Exception as e:
        logging.error(f"Failed to respond with alerts message: {e}", exc_info=True)


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
    

def iqr_bucket_lines_for_counts(values, items, num_buckets=5, iqr_multiplier=1.5):
    """
    items: List[(count:int, operator:dict)]
    values: List[int] extracted from items
    """
    zero_items = [(c, op) for c, op in items if c == 0]
    non_zero_items = [(c, op) for c, op in items if c > 0]

    if not non_zero_items:
        # No positive counts: just a zero bucket
        return [], [], len(zero_items), []

    non_zero_values = [c for c, _ in non_zero_items]

    # Small sample: just 1 bucket spanning min..max
    if len(non_zero_values) < 2:
        min_val = min(non_zero_values)
        max_val = max(non_zero_values)
        return [non_zero_items], [(min_val, max_val)], len(zero_items), []

    # IQR to detect high outliers
    q1 = statistics.quantiles(non_zero_values, n=4)[0]
    q3 = statistics.quantiles(non_zero_values, n=4)[2]
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * iqr

    inliers = [(c, op) for c, op in non_zero_items if 0 < c <= upper_bound]
    outliers = [(c, op) for c, op in non_zero_items if c > upper_bound]

    # Degenerate case: everything got marked outlier; fall back to treating all as inliers.
    if not inliers:
        inliers, outliers = non_zero_items, []

    # Bucket domain
    if zero_items:
        min_val = min(c for c, _ in inliers)  # start at smallest positive (zeros are their own bucket)
    else:
        min_val = min(c for c, _ in (inliers + outliers))
    max_val = max(c for c, _ in inliers)

    # Guard against zero-width
    if max_val == min_val:
        num_buckets = 1
        bucket_size = 1
    else:
        bucket_size = (max_val - min_val) / (num_buckets or 1)

    # Build bucket ranges
    buckets = [[] for _ in range(num_buckets)]
    bucket_ranges = []
    for i in range(num_buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        bucket_ranges.append((lower, upper))

    # Assign inliers to buckets
    for c, op in inliers:
        if bucket_size > 0:
            idx = int((c - min_val) / bucket_size)
        else:
            idx = 0
        if idx == num_buckets:
            idx -= 1
        buckets[idx].append((c, op))

    # Sanity check
    assert len(zero_items) + sum(len(b) for b in buckets) + len(outliers) == len(items), \
        "Mismatch in total operator counts"

    return buckets, bucket_ranges, len(zero_items), outliers


def render_bucket_lines_counts(buckets_with_ranges, zero_count, outliers, items, mean, median, max_segments=20):
    """
    buckets_with_ranges: List[(bucket:list[(count, op)], lower:float, upper:float)]
    items: original list[(count, op)]
    mean/median: of counts
    Renders a bar per bucket where bar length is number of operators in that range.
    """
    def build_bar(n):
        if n == 0:
            return ""
        return "■" * max(1, int((n / max_count) * max_segments))

    def fmt_range(lower, upper):
        return f"{int(round(lower))}–{int(round(upper))}"

    max_count = max([len(b) for b, _, _ in buckets_with_ranges] + [zero_count, len(outliers), 1])

    # Precompute widths for nice alignment
    all_counts = [zero_count] + [len(b) for b, _, _ in buckets_with_ranges] + [len(outliers)]
    count_width = max(len(str(c)) for c in all_counts) + 2

    labels = ["0"]
    for _, lower, upper in buckets_with_ranges:
        labels.append(fmt_range(lower, upper))
    if outliers:
        labels.append(f">= {int(min(c for c, _ in outliers))}")
    label_width = max(len(label) for label in labels) + 1

    rows = []

    # Zero bucket
    if zero_count > 0:
        bar = build_bar(zero_count)
        markers = []
        if mean is not None and mean == 0:
            markers.append("mean")
        if median is not None and median == 0:
            markers.append("median")
        marker_str = f"⟵ {', '.join(markers)}" if markers else ""
        count_str = f"({zero_count})"
        rows.append(f"{'0':>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {marker_str}")

    # Inlier buckets
    for b, lower, upper in buckets_with_ranges:
        label = fmt_range(lower, upper)
        b_len = len(b)
        bar = build_bar(b_len)

        markers = []
        if mean is not None and lower <= mean <= upper:
            markers.append("mean")
        if median is not None and lower <= median <= upper:
            markers.append("median")
        marker_str = f"⟵ {', '.join(markers)}" if markers else ""

        count_str = f"({b_len})"
        rows.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {marker_str}")

    # Outliers (if any)
    if outliers:
        count = len(outliers)
        bar = build_bar(count)
        out_min = int(min(c for c, _ in outliers))
        out_max = int(max(c for c, _ in outliers))
        count_str = f"({count})"
        if out_min == out_max:
            out_info = f"⟵ outlier{'s' if count != 1 else ''}: {out_min}"
        else:
            out_info = f"⟵ outliers: {out_min}-{out_max}"
        label = f">= {out_min}"
        rows.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} {out_info}")

    # Wrap in code block and bundle
    lines = ["```"] + rows + ["```"]
    return bundle_messages(lines)


def compile_operator_messages(operators_data, extra_message=None, availability="all", verified="all", num_segments=20):
    """
    operator_data: dict[op_id] -> operator dict with FIELD_VALIDATOR_COUNT, FIELD_IS_PRIVATE, FIELD_IS_VO, etc.
    """
    messages = []

    all_items = []
    public_items, private_items = [], []
    verified_items, unverified_items = [], []
    public_vo_items, public_non_vo_items = [], []
    private_vo_items, private_non_vo_items = [], []

    for op in operators_data.values():
        count = op.get(FIELD_VALIDATOR_COUNT)
        if count is None:
            continue  # skip unknowns
        item = (int(count), op)
        all_items.append(item)

        is_private = bool(op.get(FIELD_IS_PRIVATE))
        is_vo = bool(op.get(FIELD_IS_VO))

        if is_vo:
            verified_items.append(item) 
        else:
            unverified_items.append(item)

        if is_private:
            private_items.append(item)
            (private_vo_items if is_vo else private_non_vo_items).append(item)
        else:
            public_items.append(item)
            (public_vo_items if is_vo else public_non_vo_items).append(item)

    def summarize(label, items, num_buckets=10, iqr_multiplier=1.5, num_segments=20):
        if not items:
            return [f"No {label} operators found."]

        values = [c for c, _ in items]
        n_ops = len(values)

        mean = statistics.mean(values)
        median = statistics.median(values)

        hi = max(values)
        hi_ops = [op for c, op in items if c == hi]
        hi_example = random.choice(hi_ops)
        hi_others = max(0, len(hi_ops) - 1)
        highest_line = (
            f"- Most active validators: {hi:,} — "
            f"{hi_example[FIELD_OPERATOR_NAME]} (ID: {hi_example[FIELD_OPERATOR_ID]})"
            + (f" and {hi_others} other{'s' if hi_others != 1 else ''}" if hi_others > 0 else "")
        )

        buckets, bucket_ranges, zero_count, outliers = iqr_bucket_lines_for_counts(
            values, items, num_buckets=num_buckets, iqr_multiplier=iqr_multiplier
        )
        bucket_lines = render_bucket_lines_counts(
            buckets_with_ranges=[(b, lo, hi_) for b, (lo, hi_) in zip(buckets, bucket_ranges)],
            zero_count=zero_count,
            outliers=outliers,
            items=items,
            max_segments=num_segments,
            mean=mean,
            median=median
        )

        public_count = sum(1 for _, op in items if not op.get(FIELD_IS_PRIVATE))
        verified_count = sum(1 for _, op in items if op.get(FIELD_IS_VO))
        public_verified_count = sum(1 for _, op in items if not op.get(FIELD_IS_PRIVATE) and op.get(FIELD_IS_VO))

        lines = [
            f"**{label} Operators**",
            f"*{n_ops} operators*",                
            f"- Operators w/ active validators: {n_ops - zero_count}",
            f"- Mean active validators per operator: {mean:.2f}",
            f"- Median active validators per operator: {int(median) if median == int(median) else round(median, 2)}",
            highest_line,        
        ]

        if availability == 'all':
            lines.append(f"- Public operators: {public_count} ({(public_count / n_ops * 100):.2f}%)")

        if verified == 'all':
            lines.append(f"- Verified operators: {verified_count} ({(verified_count / n_ops * 100):.2f}%)")

        if availability in ('all', 'public') and verified in ('all', 'verified'):
            lines.append(f"- Public verified operators: {public_verified_count} ({(public_verified_count / n_ops * 100):.2f}%)")

        lines.append(f"### {label} Active Validator Distribution Across Operators")

        lines += bucket_lines
        return bundle_messages(lines)

    if availability == "all":
        logging.debug(f"Filtered to all availability")
        if verified == "all":
            logging.debug(f"All items count: {len(all_items)}")
            messages.extend(summarize("All", all_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified == "verified":
            logging.debug(f"Public VO items count: {len(verified_items)}")
            messages.extend(summarize("All Verified", verified_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified == "unverified":
            logging.debug(f"Public non-VO items count: {len(public_non_vo_items)}")
            messages.extend(summarize("All Unverified", unverified_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))

    # Public breakdown
    if availability == "public":
        logging.debug(f"Filtered to public items")
        if verified == "all":
            logging.debug(f"Public items count: {len(public_items)}")
            messages.extend(summarize("All Public", public_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified == "verified":
            logging.debug(f"Public VO items count: {len(public_vo_items)}")
            messages.extend(summarize("Public Verified", public_vo_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))
        if verified == "unverified":
            logging.debug(f"Public non-VO items count: {len(public_non_vo_items)}")
            messages.extend(summarize("Public Unverified", public_non_vo_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments))

    # Private breakdown
    if availability == "private":
        logging.debug(f"Filtered to private items")
        if verified == "all":
            logging.debug(f"Private items count: {len(private_items)}")
            messages.extend(summarize("All Private", private_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))
        if verified == "verified":
            logging.debug(f"Private VO items count: {len(private_vo_items)}")
            messages.extend(summarize("Private Verified", private_vo_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))
        if verified == "unverified":
            logging.debug(f"Private non-VO items count: {len(private_non_vo_items)}")
            messages.extend(summarize("Private Unverified", private_non_vo_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments))

    if extra_message:
        messages.append(extra_message)

    return bundle_messages(messages)

async def respond_operator_messages(ctx, operator_data, extra_message=None, availability="all", verified="all", num_segments=20):
    try:
        messages = compile_operator_messages(
            operator_data, availability=availability, verified=verified,
            extra_message=extra_message,
            num_segments=num_segments
        )
        if messages:
            for message in messages:
                # Note: assumes ctx.defer() already called by the command handler.
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            await ctx.followup.send("Validator data not found.", ephemeral=True)
    except Exception as e:
        logging.error(f"Failed to respond with validator data message: {e}", exc_info=True)