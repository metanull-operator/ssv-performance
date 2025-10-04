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
)
#import discord
import statistics
import random


def summarize(label, fees, num_buckets=5, iqr_multiplier=1.5, num_segments=20):
    if not fees:
        return [f"No {label} operators found."]

    values = [f[0] for f in fees]
    sorted_fees = sorted(fees, key=lambda x: x[0])
    highest = sorted_fees[-1]
    lowest = sorted_fees[0]
    count = len(values)

    # Get structured buckets
    buckets, bucket_ranges, zero_count, outliers = iqr_bucketize(
        fees,
        value_fn=lambda it: it[0],        
        num_buckets=num_buckets,
        iqr_multiplier=iqr_multiplier,
        treat_zero_separately=True
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