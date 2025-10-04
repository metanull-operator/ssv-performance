#from bot.bot_operator_threshold_alerts import *
#from collections import defaultdict
#from datetime import datetime, timedelta
from common.config import (
#    OPERATOR_24H_HISTORY_COUNT,
#    ALERTS_THRESHOLDS_30D,
#    ALERTS_THRESHOLDS_24H,
#    FIELD_OPERATOR_REMOVED,
#    FIELD_OPERATOR_NAME,
#    FIELD_OPERATOR_ID,
    FIELD_VALIDATOR_COUNT,
#    FIELD_PERFORMANCE,
)
#import discord
#import statistics
#import random

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