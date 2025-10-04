import math
from typing import Callable, Iterable, List, Sequence, Tuple, TypeVar

T = TypeVar("T")

def _percentile_linear(xs: Sequence[float], p: float) -> float:
    """0<=p<=1, linear interpolation between order statistics."""
    if not xs:
        raise ValueError("Empty data for percentile")
    s = sorted(xs)
    n = len(s)
    if n == 1:
        return s[0]
    r = p * (n - 1)
    lo = math.floor(r)
    hi = math.ceil(r)
    if lo == hi:
        return s[lo]
    frac = r - lo
    return s[lo] * (1 - frac) + s[hi] * frac

def iqr_bucketize(
    items: Iterable[T],
    value_fn: Callable[[T], float] = lambda x: x[0],   # works for (value, obj)
    num_buckets: int = 5,
    iqr_multiplier: float = 1.5,
    treat_zero_separately: bool = True,
):
    """
    Returns: (buckets, bucket_ranges, zero_count, outliers)

    - buckets: List[List[item]] of inliers split into num_buckets ranges
    - bucket_ranges: List[(lower: float, upper: float)]
    - zero_count: int  (# of items with value == 0, if treat_zero_separately)
    - outliers: List[item]  (items with value > Q3 + k*IQR)
    """
    items = list(items)
    vals = [float(value_fn(it)) for it in items]

    # Split zeros (if requested)
    if treat_zero_separately:
        zero_mask = [v == 0 for v in vals]
        zero_count = sum(zero_mask)
        non_zero_items = [it for it, is_zero in zip(items, zero_mask) if not is_zero]
        non_zero_vals  = [float(value_fn(it)) for it in non_zero_items]
    else:
        zero_count = 0
        non_zero_items = items
        non_zero_vals  = vals

    if not non_zero_items:
        # Only zeros present
        return [], [], zero_count, []

    # Small-sample fallback: 1 bucket spanning min..max (inliers only)
    if len(non_zero_vals) < 2:
        mn = min(non_zero_vals)
        mx = max(non_zero_vals)
        return [non_zero_items], [(mn, mx)], zero_count, []

    # IQR-based high-outlier detection
    q1 = _percentile_linear(non_zero_vals, 0.25)
    q3 = _percentile_linear(non_zero_vals, 0.75)
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * max(iqr, 0.0)

    inliers = [it for it in non_zero_items if 0 < float(value_fn(it)) <= upper_bound]
    outliers = [it for it in non_zero_items if float(value_fn(it)) > upper_bound]

    # Safety net: if everything got marked outlier, treat all as inliers
    if not inliers:
        inliers, outliers = non_zero_items, []

    # Bucket domain
    inlier_vals = [float(value_fn(it)) for it in inliers]
    if treat_zero_separately and zero_count > 0:
        min_val = min(inlier_vals)  # zeros are counted separately
    else:
        # include smallest among all positives (inliers + outliers) for left edge
        all_pos_vals = inlier_vals + [float(value_fn(it)) for it in outliers]
        min_val = min(all_pos_vals) if all_pos_vals else min(inlier_vals)
    max_val = max(inlier_vals)

    # Guard zero-width domain
    if max_val == min_val or num_buckets <= 1:
        buckets = [list(inliers)]  # single bucket
        bucket_ranges = [(min_val, max_val if max_val > min_val else min_val + 1.0)]
        # Sanity check
        assert zero_count + len(buckets[0]) + len(outliers) == len(items), \
            "Mismatch in total counts"
        return buckets, bucket_ranges, zero_count, outliers

    # Build ranges
    bucket_size = (max_val - min_val) / float(num_buckets)
    buckets: List[List[T]] = [[] for _ in range(num_buckets)]
    bucket_ranges: List[Tuple[float, float]] = []
    for i in range(num_buckets):
        lower = min_val + i * bucket_size
        upper = lower + bucket_size
        bucket_ranges.append((lower, upper))

    # Assign inliers to buckets (clamp right-edge)
    for it in inliers:
        v = float(value_fn(it))
        idx = int((v - min_val) / bucket_size) if bucket_size > 0 else 0
        if idx == num_buckets:
            idx -= 1
        buckets[idx].append(it)

    # Sanity check: zeros + inliers + outliers equals all
    assert zero_count + sum(len(b) for b in buckets) + len(outliers) == len(items), \
        "Mismatch in total counts"

    return buckets, bucket_ranges, zero_count, outliers
