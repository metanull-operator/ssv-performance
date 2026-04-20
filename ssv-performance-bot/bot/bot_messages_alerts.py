from collections import defaultdict
from datetime import date, datetime, timezone

from bot.bot_mentions import create_subscriber_mentions, mention_role
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

    logging.debug(f"Creating 24h alerts for {len(perf_data)} operators")

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

        if operator.get(FIELD_OPERATOR_REMOVED):
            continue

        for threshold in thresholds_24h:
            alert_list = alert_msgs_24h[threshold]
            result = operator_threshold_alert_24h(operator, threshold)
            if result and validator_count_int > 0:
                operator_ids.append(result[FIELD_OPERATOR_ID])
                performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_str}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count_int})"
                alert_list.append(alert)

    return operator_ids, alert_msgs_24h


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

        if operator.get(FIELD_OPERATOR_REMOVED):
            continue

        trend_icon = get_30d_trend_icon(operator)

        for threshold in thresholds_30d:
            alert_list = alert_msgs_30d[threshold]
            result = operator_threshold_alert_30d(operator, threshold)
            if result and validator_count_int > 0:
                operator_ids.append(result[FIELD_OPERATOR_ID])
                performance_str = "N/A" if result['Performance Data Point'] is None else f"{result['Performance Data Point']}"
                performance_display = f"{performance_str}{trend_icon}" if trend_icon else performance_str
                alert = f"- {result[FIELD_OPERATOR_NAME]} - {performance_display}    (ID: {result[FIELD_OPERATOR_ID]}, Validators: {validator_count_int})"
                alert_list.append(alert)

    return operator_ids, alert_msgs_30d


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
def build_removed_operator_messages(removed_operator_ids, perf_data: dict) -> list[str]:
    if not removed_operator_ids:
        logging.debug("No removed operators triggered alerts.")
        return []

    sorted_removed = sorted(removed_operator_ids)

    removed_lines = []
    for op_id in sorted_removed:
        operator = perf_data.get(op_id)
        if not operator:
            continue

        validator_count = operator.get(FIELD_VALIDATOR_COUNT)
        validator_count_int = _as_int(validator_count)
        validator_display = validator_count if validator_count_int is None else validator_count_int
        removed_lines.append(f"- {operator[FIELD_OPERATOR_NAME]} (ID: {op_id}, Validators: {validator_display})")

    if not removed_lines:
        return []

    removed_section = ["\n**__Removed Operators with Active Validators__**"] + removed_lines
    return bundle_messages(removed_section)


##
## Build a daily-alert section listing operators whose verified status was
## removed by the collector's staleness sweep within the last
## VO_RECENTLY_REMOVED_DAYS. Each operator stays in this section for that
## many days so a missed daily post doesn't cause the notification to be
## lost entirely. Title and footnote pluralize based on count.
##
def build_recently_removed_operator_messages(perf_data: dict, today: date | None = None) -> list[str]:
    if today is None:
        today = datetime.now(timezone.utc).date()

    window = VO_RECENTLY_REMOVED_DAYS
    removed = []
    for op_id, data in perf_data.items():
        demoted_at = data.get(FIELD_VO_DEMOTED_AT)
        if demoted_at is None:
            continue
        days_since = (today - demoted_at).days
        if 0 <= days_since < window:
            removed.append((op_id, data, demoted_at))

    if not removed:
        return []

    # Most recent demotions first, ties broken by operator ID.
    removed.sort(key=lambda x: (-x[2].toordinal(), x[0]))

    count = len(removed)
    title_noun = "Operator" if count == 1 else "Operators"
    subject = "This operator's record has" if count == 1 else "These operators' records have"

    lines = [f"\n**__Recently Removed Verified {title_noun}__**"]
    for op_id, data, demoted_at in removed:
        validator_count = _as_int(data.get(FIELD_VALIDATOR_COUNT))
        validator_suffix = (
            f", Validators: {validator_count}" if validator_count and validator_count > 0 else ""
        )
        lines.append(
            f"- {data.get(FIELD_OPERATOR_NAME, '')} "
            f"(ID: {op_id}, Removed: {demoted_at.isoformat()}{validator_suffix})"
        )
    lines.append(
        f"-# {subject} not been updated in the SSV API for at least "
        f"{VO_STALENESS_DAYS} days; verified status has been removed."
    )

    return bundle_messages(lines)


##
## Compile alerts, mentions and any extra message into a single set of separate messages to be sent to Discord.
## This attempts to push everything into as few messages as possible to not bomb Discord with excessive messages.
##
def compile_vo_threshold_messages(perf_data, extra_message=None, subscriptions=None, guild=None, dm_recipients=[], mention_periods=[], include_removed=True):

    # Get list of operators and alert messages for each period.
    operator_ids_24h, alerts_24h = create_alerts_24h(perf_data)
    operator_ids_30d, alerts_30d = create_alerts_30d(perf_data)

    removed_operator_ids = [op_id for op_id, data in perf_data.items() if data.get(FIELD_OPERATOR_REMOVED)]

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
    mentions_removed = []
    if include_removed:
        removed_bundles = build_removed_operator_messages(removed_operator_ids, perf_data)
        messages.extend(removed_bundles)

        if subscriptions and guild and 'removed' in mention_periods:
            mentions_removed = create_subscriber_mentions(guild, subscriptions, removed_operator_ids, 'alerts', dm_recipients)

    # Operators whose verified status was removed by the collector sweep
    # within the last VO_RECENTLY_REMOVED_DAYS.
    recently_removed_msgs = build_recently_removed_operator_messages(perf_data)
    messages.extend(recently_removed_msgs)

    role_mention = ''
    if recently_removed_msgs and guild and ALERT_RECENTLY_REMOVED_ROLE:
        role_mention = mention_role(guild, ALERT_RECENTLY_REMOVED_ROLE).strip()

    mentions = list(dict.fromkeys(mentions_24h + mentions_30d + mentions_removed))
    if role_mention:
        if mentions:
            # Glue onto the first mention entry so the role ping shares a line
            # with the subscriber pings (each mention entry already starts with
            # "\n", and bundle_messages inserts another "\n" between entries,
            # which would otherwise produce a blank line between them).
            mentions[0] = "\n" + role_mention + " " + mentions[0].lstrip("\n")
        else:
            mentions.append("\n" + role_mention)
    messages.extend(mentions)

    # Include an extra message, if configured
    if extra_message and len(extra_message) > 0:
        messages.append("\n" + extra_message)

    # Rebundle everything up again to reduce down to the fewest messages to post to Discord
    bundles = bundle_messages(messages)

    return(bundles)


##
## Send VO threshold alert messages to a channel, optionally mentioning
## users subscribed to operators that triggered alerts. Used for the 
## periodic alert messages sent to a channel by the bot loop.
##
async def send_vo_threshold_messages(channel, perf_data, extra_message=None, subscriptions=None, dm_recipients=[], mention_periods=[], include_removed=True):

    try:
        # Only attempt @mentions if we have a guild to query and subscription info
        if channel and hasattr(channel, 'guild') and subscriptions:
            messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message, subscriptions=subscriptions, guild=channel.guild, dm_recipients=dm_recipients, mention_periods=mention_periods, include_removed=include_removed)
        else:
            messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message, dm_recipients=dm_recipients, include_removed=include_removed)

        if messages:
            for message in messages:
                logging.debug(f"VO threshold alert message to send:\n{message.strip()}")
                await channel.send(message.strip())
        else:
            current_date = datetime.now().strftime("%Y-%m-%d")
            await channel.send(f'No performance alerts for {current_date}.')
    except Exception as e:
        logging.error(f"Failed to send VO threshold messages: {e}", exc_info=True)


##
## Compile messages listing removed operators with active validators.
## Used by the /removed-validators slash command.
##
def compile_removed_validators_messages(perf_data, extra_message=None):

    removed_operator_ids = [op_id for op_id, data in perf_data.items() if data.get(FIELD_OPERATOR_REMOVED)]
    messages = build_removed_operator_messages(removed_operator_ids, perf_data)

    # Include an extra message, if configured
    if extra_message and len(extra_message) > 0:
        messages.append("\n" + extra_message)

    return bundle_messages(messages) if messages else []


##
## Send removed-validators messages in response to a slash command.
## Assumption is that ctx.defer() was previously called to allow time
## for processing.
##
async def respond_removed_validators_messages(ctx, perf_data, extra_message=None):

    try:
        messages = compile_removed_validators_messages(perf_data, extra_message=extra_message)

        if messages:
            for message in messages:
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            current_date = datetime.now().strftime("%Y-%m-%d")
            await ctx.followup.send(f'No removed operators with active validators for {current_date}.', ephemeral=False)
    except Exception as e:
        logging.error(f"Failed to respond with removed validators message: {e}", exc_info=True)


##
## Send VO threshold alert messages in response to a slash command.
## Assumption is that ctx.defer() was previously called to allow time
## for processing.
##
async def respond_vo_threshold_messages(ctx, perf_data, extra_message=None):

    try:
        # Pass guild so the Recently Removed Verified Operator(s) committee
        # role mention still fires from the slash command. Subscriptions and
        # mention_periods remain unset, so per-user @-mentions don't re-ping.
        guild = getattr(ctx, 'guild', None)
        messages = compile_vo_threshold_messages(perf_data, extra_message=extra_message, guild=guild)

        if messages:
            for message in messages:
                # Note assumption that defer() was previously called.
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            current_date = datetime.now().strftime("%Y-%m-%d")
            await ctx.followup.send(f'No performance alerts for {current_date}.', ephemeral=False)
    except Exception as e:
        logging.error(f"Failed to respond with alerts message: {e}", exc_info=True)