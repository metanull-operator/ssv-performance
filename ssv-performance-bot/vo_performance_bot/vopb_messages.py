from vo_performance_bot.vopb_mentions import create_subscriber_mentions
from vo_performance_bot.vopb_subscriptions import get_user_subscriptions_by_type
from vo_performance_bot.vopb_operator_threshold_alerts import *
from datetime import datetime, timedelta
from common.config import OPERATOR_24H_HISTORY_COUNT, ALERTS_THRESHOLDS_30D, ALERTS_THRESHOLDS_24H
import discord
import statistics


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


def iqr_bucket_lines(values, fees, num_buckets=5):
    q1 = statistics.quantiles(values, n=4)[0]
    q3 = statistics.quantiles(values, n=4)[2]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Clip to actual min/max if IQR range is too narrow
    inlier_values = [v for v in values if lower_bound <= v <= upper_bound]
    if len(set(inlier_values)) < num_buckets:
        return dynamic_bucket_lines(values, fees, num_buckets)

    min_val = min(inlier_values)
    max_val = max(inlier_values)
    bucket_size = (max_val - min_val) / num_buckets

    buckets = [[] for _ in range(num_buckets)]
    outliers = []

    for fee, op in fees:
        if fee < lower_bound or fee > upper_bound:
            outliers.append((fee, op))
            continue
        i = int((fee - min_val) / bucket_size)
        if i == num_buckets:  # edge case for max_val
            i -= 1
        buckets[i].append(fee)

    max_count = max(len(b) for b in buckets) or 1
    lines = []
    for i, b in enumerate(buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        bar = "█" * int((len(b) / max_count) * 20)
        lines.append(f"{lower:.2f}–{upper:.2f} SSV  {bar:<20} ({len(b)})")

    if outliers:
        lines.append(f"{len(outliers)} operators above/below typical range (outliers not shown)")

    return lines


def iqr_bucket_lines_with_outlier_summary(values, fees, num_buckets=5):
    q1 = statistics.quantiles(values, n=4)[0]
    q3 = statistics.quantiles(values, n=4)[2]
    iqr = q3 - q1
    upper_bound = q3 + 1.5 * iqr

    # Only consider fees ≥ 0 for main buckets
    inlier_fees = [(fee, op) for fee, op in fees if fee <= upper_bound]
    outlier_fees = [(fee, op) for fee, op in fees if fee > upper_bound]

    if len(set(fee for fee, _ in inlier_fees)) < num_buckets:
        return dynamic_bucket_lines(values, fees, num_buckets)

    inlier_values = [fee for fee, _ in inlier_fees]
    min_val = min(inlier_values)
    max_val = max(inlier_values)
    bucket_size = (max_val - min_val) / num_buckets

    buckets = [[] for _ in range(num_buckets)]
    for fee, _ in inlier_fees:
        i = int((fee - min_val) / bucket_size)
        if i == num_buckets:  # edge case for max_val
            i -= 1
        buckets[i].append(fee)

    max_count = max(len(b) for b in buckets) or 1
    lines = []
    for i, b in enumerate(buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        bar = "█" * int((len(b) / max_count) * 20)
        lines.append(f"{lower:.2f}–{upper:.2f} SSV  {bar:<20} ({len(b)})")

    if outlier_fees:
        outlier_count = len(outlier_fees)
        outlier_min = min(fee for fee, _ in outlier_fees)
        outlier_max = max(fee for fee, _ in outlier_fees)
        lines.append(f"> {upper_bound:.2f} SSV  {'█' * 20} ({outlier_count}) — High-end outliers {outlier_min:.2f}–{outlier_max:.2f} SSV")

    return lines


def iqr_bucket_lines_with_zero_handling(values, fees, num_buckets=5, iqr_multiplier=1.5):
    zero_fees = [(fee, op) for fee, op in fees if fee == 0]
    non_zero_fees = [(fee, op) for fee, op in fees if fee > 0]

    if not non_zero_fees:
        return [], [], len(zero_fees), []

    non_zero_values = [fee for fee, _ in non_zero_fees]

    # IQR-based outlier detection
    q1 = statistics.quantiles(non_zero_values, n=4)[0]
    q3 = statistics.quantiles(non_zero_values, n=4)[2]
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * iqr

    inlier_fees = [(fee, op) for fee, op in non_zero_fees if fee <= upper_bound]
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

    for fee, _ in inlier_fees:
        i = int((fee - min_val) / bucket_size)
        if i == num_buckets:
            i -= 1
        buckets[i].append(fee)

    return buckets, bucket_ranges, len(zero_fees), outlier_fees


def iqr_bucket_lines_with_zero_handling1(values, fees, num_buckets=5, iqr_multiplier=1.5):
    zero_fees = [(fee, op) for fee, op in fees if fee == 0]
    non_zero_fees = [(fee, op) for fee, op in fees if fee > 0]

    if not non_zero_fees:
        return [f"All operators charge 0.00 SSV."]

    non_zero_values = [fee for fee, _ in non_zero_fees]

    # IQR-based outlier detection
    q1 = statistics.quantiles(non_zero_values, n=4)[0]
    q3 = statistics.quantiles(non_zero_values, n=4)[2]
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * iqr

    inlier_fees = [(fee, op) for fee, op in non_zero_fees if fee <= upper_bound]
    outlier_fees = [(fee, op) for fee, op in non_zero_fees if fee > upper_bound]

    if zero_fees:
        min_val = min(fee for fee, _ in inlier_fees)
    else:
        # No 0.00 SSV operators, so start from actual minimum fee in all data
        min_val = min(fee for fee, _ in inlier_fees + outlier_fees)
    max_val = max(fee for fee, _ in inlier_fees)
    bucket_size = (max_val - min_val) / (num_buckets or 1)

    # Build buckets
    buckets = [[] for _ in range(num_buckets)]
    for fee, _ in inlier_fees:
        i = int((fee - min_val) / bucket_size)
        if i == num_buckets:
            i -= 1
        buckets[i].append(fee)

    max_count = max(
        [len(b) for b in buckets] + [len(zero_fees)]
    ) or 1  # include 0-fee count in scaling

    lines = []
    if zero_fees:
        bar = "█" * int((len(zero_fees) / max_count) * 20)
        count_str = f"({len(zero_fees)})"
        lines.append(f"0.00 SSV       {bar:<20} {count_str:<16}")

    for i, b in enumerate(buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        b_len = len(b)
        bar = "█" * int((b_len / max_count) * 20)
        count_str = f"({b_len})"
        lines.append(f"{lower:.2f}–{upper:.2f} SSV  {bar:<20}  {count_str:<16})")

    if outlier_fees:
        outlier_count = len(outlier_fees)
        outlier_min = min(fee for fee, _ in outlier_fees)
        outlier_max = max(fee for fee, _ in outlier_fees)
        lines.append(f"Outliers > {upper_bound:.2f} SSV  {'█' * 20} ({outlier_count}) ({outlier_min:.2f}–{outlier_max:.2f})")

    return lines


def render_bucket_lines(buckets_with_ranges, zero_count, outliers, fees, mean, median, max_segments=20):
    max_count = max([len(b) for b, _, _ in buckets_with_ranges] + [zero_count, len(outliers)])
    lines = []

    def build_bar(count):
        if count == 0:
            return ""
        return "■" * max(1, int((count / max_count) * max_segments))

    max_fee = max([fee for fee, _ in fees])
    max_label = f"Outliers > {max_fee:.2f}"
    label_width = len(max_label) + 1

    count_width = 8

    if zero_count > 0:
        bar = build_bar(zero_count)

        markers = []
        if mean is not None and mean == 0:
            markers.append("mean")
        if median is not None and median == 0:
            markers.append("median")

        marker_str = f"⟵ {', '.join(markers)}" if markers else ""
        count_str = f"({zero_count})"
        lines.append(f"{'0.00':>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}}{marker_str}")


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
        count_str = f"({b_len})"
        lines.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}}{marker_str}")

    if outliers:
        count = len(outliers)
        outlier_min = min(fee for fee, _ in outliers)
        outlier_max = max(fee for fee, _ in outliers)
        bar = build_bar(count)
        count_str = f"({count})"
        label = f"Outliers > {outlier_min:.2f}"
        lines.append(f"{label:>{label_width}} {bar:<{max_segments}} {count_str:<{count_width}} ({outlier_min:.2f}-{outlier_max:.2f})")

    lines = ["```"] + lines + ["```"]

    return lines


def dynamic_bucket_lines(values, fees, num_buckets=10):
    min_fee = min(values)
    max_fee = max(values)
    if min_fee == max_fee:
        # Degenerate case: all values the same
        return [f"All operators charge the same fee: {min_fee:.2f} SSV"]

    bucket_size = (max_fee - min_fee) / num_buckets
    bucket_counts = [0 for _ in range(num_buckets)]
    bucket_ranges = []

    # Build ranges like: (0.0, 2.0), (2.0, 4.0), ...
    for i in range(num_buckets):
        lower = min_fee + i * bucket_size
        upper = lower + bucket_size
        bucket_ranges.append((lower, upper))

    # Count values in each bucket
    for val in values:
        for i, (lower, upper) in enumerate(bucket_ranges):
            if i == num_buckets - 1:
                # Include upper bound in last bucket
                if lower <= val <= upper:
                    bucket_counts[i] += 1
                    break
            elif lower <= val < upper:
                bucket_counts[i] += 1
                break

    max_count = max(bucket_counts) or 1
    lines = []
    for i, (lower, upper) in enumerate(bucket_ranges):
        count = bucket_counts[i]
        bar = "█" * int((count / max_count) * 20)
        lines.append(f"{lower:.2f}–{upper:.2f} SSV  {bar:<20} ({count})")

    return lines


def compile_fee_messages(fee_data, extra_message=None):
    messages = []

    public_fees = []
    private_fees = []

    for operator in fee_data.values():
        fee = operator.get(FIELD_OPERATOR_FEE)
        is_private = operator.get(FIELD_IS_PRIVATE)
        if fee is None:
            continue
        if is_private:
            private_fees.append((fee, operator))
        else:
            public_fees.append((fee, operator))

    def summarize(label, fees, num_buckets=5, iqr_multiplier=1.5):
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
            max_segments=40,
            mean=mean,
            median=median
        )

        return [
            f"**{label} Operators (SSV/year)**",
            f"*{count} operators*",
            f"- Mean Fee: {mean:.2f}",
            f"- Median Fee: {median:.2f}",
            f"- Lowest Fee: {lowest[0]:.2f} - {lowest[1][FIELD_OPERATOR_NAME]} (ID: {lowest[1][FIELD_OPERATOR_ID]}, Validators: {lowest[1][FIELD_VALIDATOR_COUNT]})",
            f"- Highest Fee: {highest[0]:.2f} - {highest[1][FIELD_OPERATOR_NAME]} (ID: {highest[1][FIELD_OPERATOR_ID]}, Validators: {highest[1][FIELD_VALIDATOR_COUNT]})",
            "### Fee Distribution"
        ] + bucket_lines


    messages.extend(summarize("Public", public_fees, iqr_multiplier=1.5, num_buckets=10))
    messages.append("")  # spacing
    messages.extend(summarize("Private", private_fees, iqr_multiplier=2.5, num_buckets=5))

    if extra_message:
        messages.append("")
        messages.append(extra_message)

    return bundle_messages(messages)


async def respond_fee_messages(ctx, fee_data, extra_message=None):
    try:
        messages = compile_fee_messages(fee_data, extra_message=extra_message)

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