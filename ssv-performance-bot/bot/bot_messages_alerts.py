from collections import defaultdict
from datetime import datetime

from bot.bot_mentions import create_subscriber_mentions
from bot.bot_messages import bundle_messages
from bot.bot_operator_threshold_alerts import *
from common.config import (
    ALERTS_THRESHOLDS_30D,
    ALERTS_THRESHOLDS_24H,
    FIELD_OPERATOR_REMOVED,
    FIELD_OPERATOR_NAME,
    FIELD_OPERATOR_ID,
    FIELD_VALIDATOR_COUNT,
    FIELD_PERFORMANCE,
)


def create_alerts_24h(perf_data):
    thresholds_24h = sorted(ALERTS_THRESHOLDS_24H, reverse=True)
    alert_msgs_24h = {threshold: [] for threshold in thresholds_24h}
    operator_ids = []
    removed_alerts = defaultdict(lambda: defaultdict(set))

    logging.debug(f"Creating 24h alerts for {len(perf_data)} operators")

    for op_id, operator in perf_data.items():

        logging.debug(f"Creating 24h alerts for operator {op_id}")

        if not operator[FIELD_IS_VO]:
            continue

        validator_count = operator[FIELD_VALIDATOR_COUNT]
        if validator_count is None:
            continue

        try:
            validator_count_int = int(validator_count)
        except (TypeError, ValueError):
            logging.debug(f"Operator {op_id} validator count not numeric: {validator_count}")
            continue

        if validator_count_int <= 0:
            continue

        is_removed = bool(operator.get(FIELD_OPERATOR_REMOVED))

        for threshold in thresholds_24h:
            alert_list = alert_msgs_24h[threshold]
            logging.debug(f"Checking operator {op_id} against threshold {threshold}")
            result = operator_threshold_alert_24h(operator, threshold)
            if result and validator_count_int > 0:
                if is_removed:
                    removed_alerts[result[FIELD_OPERATOR_ID]]['24h'].add(threshold)
                else:
                    operator_ids.append(result[FIELD_OPERATOR_ID])
                    performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                    alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_str}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count_int})"
                    alert_list.append(alert)

    return operator_ids, alert_msgs_24h, removed_alerts


def get_30d_trend_icon(operator):
    perf_points = operator.get(FIELD_PERFORMANCE, {}) or {}
    perf_24h = perf_points.get('24h')
    perf_30d = perf_points.get('30d')

    try:
        if perf_24h is None or perf_30d is None:
            return ''
        perf_24h = float(perf_24h)
        perf_30d = float(perf_30d)
    except (TypeError, ValueError):
        return ''

    if perf_24h > perf_30d:
        return " ↗︎"  # green up arrow
    if perf_24h < perf_30d:
        return " ↘︎"  # red down arrow
    return " →"  # steady indicator


def create_alerts_30d(perf_data):
    thresholds_30d = sorted(ALERTS_THRESHOLDS_30D, reverse=True)
    alert_msgs_30d = {threshold: [] for threshold in thresholds_30d}
    operator_ids = []
    removed_alerts = defaultdict(lambda: defaultdict(set))

    for op_id, operator in perf_data.items():

        if not operator[FIELD_IS_VO]:
            continue

        validator_count = operator[FIELD_VALIDATOR_COUNT]
        if validator_count is None:
            continue

        try:
            validator_count_int = int(validator_count)
        except (TypeError, ValueError):
            logging.debug(f"Operator {op_id} validator count not numeric: {validator_count}")
            continue

        if validator_count_int <= 0:
            continue

        is_removed = bool(operator.get(FIELD_OPERATOR_REMOVED))
        trend_icon = get_30d_trend_icon(operator)

        for threshold in thresholds_30d:
            alert_list = alert_msgs_30d[threshold]
            result = operator_threshold_alert_30d(operator, threshold)
            if result and validator_count_int > 0:
                if is_removed:
                    removed_alerts[result[FIELD_OPERATOR_ID]]['30d'].add(threshold)
                else:
                    operator_ids.append(result[FIELD_OPERATOR_ID])
                    performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                    performance_display = f"{performance_str}{trend_icon}" if trend_icon else performance_str
                    alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_display}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count_int})"
                    alert_list.append(alert)

    return operator_ids, alert_msgs_30d, removed_alerts


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

    operator_ids_24h, alerts_24h, removed_24h = create_alerts_24h(perf_data)
    operator_ids_30d, alerts_30d, removed_30d = create_alerts_30d(perf_data)

    removed_combined = defaultdict(lambda: defaultdict(set))
    for source in (removed_24h, removed_30d):
        for op_id, periods in source.items():
            for period, thresholds in periods.items():
                removed_combined[op_id][period].update(thresholds)

    messages = []

    mentions_24h = []
    messages.extend(compile_alert_threshold_groups(alerts_24h, "24h"))
    if subscriptions and guild and '24h' in mention_periods:
        mentions_24h = create_subscriber_mentions(guild, subscriptions, operator_ids_24h, 'alerts', dm_recipients)

    mentions_30d = []
    messages.extend(compile_alert_threshold_groups(alerts_30d, "30d"))
    if subscriptions and guild and '30d' in mention_periods:
        mentions_30d = create_subscriber_mentions(guild, subscriptions, operator_ids_30d, 'alerts', dm_recipients)

    def build_removed_operator_messages():
        if not removed_combined:
            return []

        def as_int(value):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        # Sort primarily by validator count to highlight operators affecting most validators
        sorted_removed = sorted(
            removed_combined.items(),
            key=lambda item: (
                -(as_int(perf_data.get(item[0], {}).get(FIELD_VALIDATOR_COUNT)) or 0),
                item[0]
            )
        )

        removed_lines = []
        for op_id, periods in sorted_removed:
            operator = perf_data.get(op_id)
            if not operator:
                continue

            validator_value = operator.get(FIELD_VALIDATOR_COUNT)
            validator_count = as_int(validator_value)
            if validator_count is None:
                validator_display = validator_value if validator_value is not None else "N/A"
            else:
                validator_display = validator_count

            perf_points = operator.get(FIELD_PERFORMANCE, {}) or {}
            trend_icon = get_30d_trend_icon(operator)
            period_chunks = []
            for period in ('24h', '30d'):
                thresholds = sorted(periods.get(period, set()), reverse=True)
                if not thresholds:
                    continue

                perf_value = perf_points.get(period)
                if perf_value is None:
                    perf_str = "N/A"
                else:
                    try:
                        perf_str = f"{float(perf_value) * 100:.2f}%"
                    except (TypeError, ValueError):
                        perf_str = "N/A"

                indicator = trend_icon if period == '30d' and trend_icon else ''
                performance_display = f"{perf_str}{indicator}" if indicator else perf_str

                threshold_str = ', '.join(f"< {t:.0%}" for t in thresholds)
                period_chunks.append(f"{period}: {performance_display} ({threshold_str})")

            if not period_chunks:
                continue

            line = f"- {operator[FIELD_OPERATOR_NAME]} (ID: {op_id}, Validators: {validator_display}) - {'; '.join(period_chunks)}"
            removed_lines.append(line)

        if not removed_lines:
            return []

        removed_section = ["\n**__Removed Operators with Active Validators__**"] + removed_lines
        return bundle_messages(removed_section)

    messages.extend(build_removed_operator_messages())

    mentions = list(dict.fromkeys(mentions_24h + mentions_30d))
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