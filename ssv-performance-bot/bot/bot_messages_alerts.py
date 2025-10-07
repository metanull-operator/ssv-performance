from collections import defaultdict
from datetime import datetime

from bot.bot_mentions import create_subscriber_mentions
from bot.bot_messages import bundle_messages
from bot.bot_operator_threshold_alerts import *
from common.config import *


##
## Compare operator performance against one or more 24h thresholds and return
## a list of affected operators and formatted alert messages. Also returns
## a dict of apparently removed operators with active validators that 
## also triggered alerts. Removed operators are displayed separately.
##
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


##
## Get an icon representing whether 30d performance is trending up, down or flat
## based on 24h performance vs 30d performance.
##
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
        return " ↗︎"  
    if perf_24h < perf_30d:
        return " ↘︎"  
    return " →" 


##
## Compare operator performance against one or more 30d thresholds and return
## a list of affected operators and formatted alert messages. Also returns
## a dict of apparently removed operators with active validators that 
## also triggered alerts. Removed operators are displayed separately.
##
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


##
## Compile alert messages grouped by threshold to minimize number of messages.
## Each group of messages is prefixed with a title indicating the threshold.
##
def compile_alert_threshold_groups(alerts, period_label):
    messages = []

    for threshold, alert_list in alerts.items():
        title = f"\n**__{period_label} < {threshold:.0%}:__**\n"
        message_bundles = bundle_messages(alert_list, MAX_DISCORD_MESSAGE_LENGTH - len(title))

        for bundle in message_bundles:
            messages.append(title + bundle)

    return messages


##
## Safely convert a value to int, returning None if conversion fails.
##
def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


##
## Build messages for removed operators that triggered alerts and bundle them.
##
def build_removed_operator_messages(removed_combined: dict, perf_data: dict) -> list[str]:
    if not removed_combined:
        return []

    # Sort by validator count desc, then op_id
    sorted_removed = sorted(
        removed_combined.items(),
        key=lambda item: (
            -(_as_int(perf_data.get(item[0], {}).get(FIELD_VALIDATOR_COUNT)) or 0),
            item[0],
        ),
    )

    removed_lines = []
    for op_id, periods in sorted_removed:
        operator = perf_data.get(op_id)
        if not operator:
            continue

        validator_count = operator.get(FIELD_VALIDATOR_COUNT)
        validator_count_int = _as_int(validator_count)
        validator_display = validator_count if validator_count_int is None else validator_count_int
        perf_points = operator.get(FIELD_PERFORMANCE, {}) or {}

        # Build indented performance lines for each period
        period_lines = []
        for period in ("24h", "30d"):
            thresholds = sorted(periods.get(period, set()), reverse=True)
            if not thresholds:
                continue

            perf_value = perf_points.get(period)
            try:
                perf_str = f"{float(perf_value) * 100:.2f}%" if perf_value is not None else "N/A"
            except (TypeError, ValueError):
                perf_str = "N/A"

            threshold_str = ", ".join(f"< {t:.0%}" for t in thresholds)
            period_lines.append(f"    {period}: {perf_str} ({threshold_str})")

        if period_lines:
            removed_lines.append(
                f"- {operator[FIELD_OPERATOR_NAME]} "
                f"(ID: {op_id}, Validators: {validator_display})\n"
                + "\n".join(period_lines)
            )

    if not removed_lines:
        return []

    removed_section = ["\n**__Removed Operators with Active Validators__**"] + removed_lines
    return bundle_messages(removed_section)


##
## Compile alerts, mentions and any extra message into a single set of separate messages to be sent to Discord.
## This attempts to push everything into as few messages as possible to not bomb Discord with excessive messages.
##
def compile_vo_threshold_messages(perf_data, extra_message=None, subscriptions=None, guild=None, dm_recipients=[], mention_periods=[]):

    # Get list of operators and alert messages for each period.
    operator_ids_24h, alerts_24h, removed_24h = create_alerts_24h(perf_data)
    operator_ids_30d, alerts_30d, removed_30d = create_alerts_30d(perf_data)

    # Combine removed operators from both periods into a single dict
    removed_combined = defaultdict(lambda: defaultdict(set))
    for source in (removed_24h, removed_30d):
        for op_id, periods in source.items():
            for period, thresholds in periods.items():
                removed_combined[op_id][period].update(thresholds)

    messages = []

    # Create message groups for 24h period and get mentions for that period
    messages.extend(compile_alert_threshold_groups(alerts_24h, "24h"))
    mentions_24h = []
    if subscriptions and guild and '24h' in mention_periods:
        mentions_24h = create_subscriber_mentions(guild, subscriptions, operator_ids_24h, 'alerts', dm_recipients)

    # Create message groups for 30d period and get mentions for that period
    messages.extend(compile_alert_threshold_groups(alerts_30d, "30d"))
    mentions_30d = []
    if subscriptions and guild and '30d' in mention_periods:
        mentions_30d = create_subscriber_mentions(guild, subscriptions, operator_ids_30d, 'alerts', dm_recipients)

    # Create messages for removed operators that triggered alerts
    removed_bundles = build_removed_operator_messages(
        removed_combined,
        perf_data,
        get_trend_icon=get_30d_trend_icon,
        bundle_messages_fn=bundle_messages,
    )
    messages.extend(removed_bundles)

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


##
## Send VO threshold alert messages to a channel, optionally mentioning
## users subscribed to operators that triggered alerts. Used for the 
## periodic alert messages sent to a channel by the bot loop.
##
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


##
## Send VO threshold alert messages in response to a slash command.
## Assumption is that ctx.defer() was previously called to allow time
## for processing.
##
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