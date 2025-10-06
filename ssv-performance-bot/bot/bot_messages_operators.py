import statistics
import random
import logging

from bot.bot_data_processing import iqr_bucketize
from bot.bot_visualizations import render_bucket_lines_counts
from bot.bot_messages import bundle_messages
from common.config import *


##
## Generate summary text for a set of operators based on their active validator counts.
##  - num_buckets: Number of buckets to create for the IQR bucketization.  
##  - iqr_multiplier: Multiplier to apply to the IQR to determine outlier thresholds.
##  - num_segments: Maximum number of segments to use in the bar chart rendering. More
##    segments means the chart will be wider on screen.
##
def generate_summary_text(label, items, num_buckets=10, iqr_multiplier=1.5, num_segments=20, availability="all", verified="all"):
    if not items:
        return [f"No {label} operators found."]

    values = [c for c, _ in items]

    # Process counts for summary lines
    n_ops = len(values)
    public_count = sum(1 for _, op in items if not op.get(FIELD_IS_PRIVATE))
    verified_count = sum(1 for _, op in items if op.get(FIELD_IS_VO))
    public_verified_count = sum(1 for _, op in items if not op.get(FIELD_IS_PRIVATE) and op.get(FIELD_IS_VO))

    # Process mean and median
    mean = statistics.mean(values)
    median = statistics.median(values)

    # Process maximum
    hi = max(values)
    hi_ops = [op for c, op in items if c == hi]
    hi_example = random.choice(hi_ops)
    hi_others = max(0, len(hi_ops) - 1)
    highest_line = (
        f"- Most active validators: {hi:,} — "
        f"{hi_example[FIELD_OPERATOR_NAME]} (ID: {hi_example[FIELD_OPERATOR_ID]})"
        + (f" and {hi_others} other{'s' if hi_others != 1 else ''}" if hi_others > 0 else "")
    )

    # Create data buckets
    buckets, bucket_ranges, zero_count, outliers = iqr_bucketize(
        items,
        value_fn=lambda it: it[0],        
        num_buckets=num_buckets,
        iqr_multiplier=iqr_multiplier,
        treat_zero_separately=True
    )

    # Begin rendering text

    lines = [
        f"**{label} Operators**",
        f"*{n_ops} operators*",                
        f"- Operators w/ active validators: {n_ops - zero_count}",
        f"- Mean active validators per operator: {mean:.2f}",
        f"- Median active validators per operator: {int(median) if median == int(median) else round(median, 2)}",
        highest_line,        
    ]

    # Selecting additional summary lines based on filters
    if availability == 'all':
        lines.append(f"- Public operators: {public_count} ({(public_count / n_ops * 100):.2f}%)")
    if verified == 'all':
        lines.append(f"- Verified operators: {verified_count} ({(verified_count / n_ops * 100):.2f}%)")
    if availability in ('all', 'public') and verified in ('verified'):
        lines.append(f"- Public verified operators: {public_verified_count} ({(public_verified_count / n_ops * 100):.2f}%)")

    lines.append(f"### {label} Active Validator Distribution Across Operators")

    # Generate chart
    bucket_lines = render_bucket_lines_counts(
        buckets_with_ranges=[(b, lo, hi_) for b, (lo, hi_) in zip(buckets, bucket_ranges)],
        zero_count=zero_count,
        outliers=outliers,
        items=items,
        max_segments=num_segments,
        mean=mean,
        median=median
    )

    lines += bucket_lines
    return bundle_messages(lines)


##
## Compile multiple messages to display operator details.
##
def compile_operator_messages(operators_data, extra_message=None, availability="all", verified="all", num_segments=20):
    messages = []

    all_items = []
    public_items, private_items = [], []
    verified_items, unverified_items = [], []
    public_vo_items, public_non_vo_items = [], []
    private_vo_items, private_non_vo_items = [], []

    # Collect counts into appropriate lists by public/private and VO status
    for op in operators_data.values():
        count = op.get(FIELD_VALIDATOR_COUNT)
        if count is None:
            continue
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

    # Generating summaries based on filters

    # Overall breakdown
    if availability == "all":
        logging.debug(f"Filtered to all availability")
        if verified == "all":
            logging.debug(f"All items count: {len(all_items)}")
            messages.extend(generate_summary_text("All", all_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "verified":
            logging.debug(f"Public VO items count: {len(verified_items)}")
            messages.extend(generate_summary_text("All Verified", verified_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "unverified":
            logging.debug(f"Public non-VO items count: {len(public_non_vo_items)}")
            messages.extend(generate_summary_text("All Unverified", unverified_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))

    # Public breakdown
    if availability == "public":
        logging.debug(f"Filtered to public items")
        if verified == "all":
            logging.debug(f"Public items count: {len(public_items)}")
            messages.extend(generate_summary_text("All Public", public_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "verified":
            logging.debug(f"Public VO items count: {len(public_vo_items)}")
            messages.extend(generate_summary_text("Public Verified", public_vo_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "unverified":
            logging.debug(f"Public non-VO items count: {len(public_non_vo_items)}")
            messages.extend(generate_summary_text("Public Unverified", public_non_vo_items, iqr_multiplier=1.5, num_buckets=10, num_segments=num_segments, availability=availability, verified=verified))

    # Private breakdown
    if availability == "private":
        logging.debug(f"Filtered to private items")
        if verified == "all":
            logging.debug(f"Private items count: {len(private_items)}")
            messages.extend(generate_summary_text("All Private", private_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "verified":
            logging.debug(f"Private VO items count: {len(private_vo_items)}")
            messages.extend(generate_summary_text("Private Verified", private_vo_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments, availability=availability, verified=verified))
        if verified == "unverified":
            logging.debug(f"Private non-VO items count: {len(private_non_vo_items)}")
            messages.extend(generate_summary_text("Private Unverified", private_non_vo_items, iqr_multiplier=2.5, num_buckets=5, num_segments=num_segments, availability=availability, verified=verified))

    if extra_message:
        messages.append(extra_message)

    return bundle_messages(messages)


##
## Send operator messages in response to a slash command.
##
async def respond_operator_messages(ctx, operator_data, extra_message=None, availability="all", verified="all", num_segments=20):
    try:
        messages = compile_operator_messages(
            operator_data, availability=availability, verified=verified,
            extra_message=extra_message,
            num_segments=num_segments
        )
        if messages:
            for message in messages:
                await ctx.followup.send(message.strip(), ephemeral=False)
        else:
            await ctx.followup.send("Validator data not found.", ephemeral=True)
    except Exception as e:
        logging.error(f"Failed to respond with validator data message: {e}", exc_info=True)