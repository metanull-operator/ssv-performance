#from bot.bot_mentions import create_subscriber_mentions
#from bot.bot_subscriptions import get_user_subscriptions_by_type
#from bot.bot_operator_threshold_alerts import *
#from collections import defaultdict
#from datetime import datetime, timedelta
from bot.bot_messages import bundle_messages
from common.config import (
#    OPERATOR_24H_HISTORY_COUNT,
#    ALERTS_THRESHOLDS_30D,
#    ALERTS_THRESHOLDS_24H,
#    FIELD_OPERATOR_REMOVED,
    FIELD_OPERATOR_NAME,
    FIELD_OPERATOR_ID,
    FIELD_VALIDATOR_COUNT,
#    FIELD_PERFORMANCE,
    FIELD_IS_PRIVATE,
    FIELD_IS_VO,
)
#import discord
from bot.bot_data_processing import iqr_bucketize
import statistics
import random
import logging


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

    buckets, bucket_ranges, zero_count, outliers = iqr_bucketize(
        items,
        value_fn=lambda it: it[0],        
        num_buckets=num_buckets,
        iqr_multiplier=iqr_multiplier,
        treat_zero_separately=True
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

    if availability in ('all', 'public') and verified in ('verified'):
        lines.append(f"- Public verified operators: {public_verified_count} ({(public_verified_count / n_ops * 100):.2f}%)")

    lines.append(f"### {label} Active Validator Distribution Across Operators")

    lines += bucket_lines
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