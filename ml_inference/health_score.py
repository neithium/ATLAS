"""
ATLAS ML Inference Pipeline — Health Score Calculator
======================================================
This module is intentionally isolated from the rest of the inference
pipeline so that the health-score formula can be changed by the team
without touching inference.py.

Three components are combined into a single score in [0, 100]:

    AHC  — Anomaly Health Component
           Normalised isolation-forest anomaly score.
           High AHC  → model considers device "normal".

    DHC  — Deviation Health Component
           How far the current MetricValue is from the batch average,
           relative to the batch range.
           High DHC  → current value is close to average (stable).

    TCC  — Temporal Consistency Component
           How well the current value matches the historical average
           for this device at this hour of the day.
           High TCC  → device follows its usual hourly pattern.

Final score:
    health_score = W1 * AHC + W2 * DHC + W3 * TCC
    clamped to [0, 100]

NOTE: This formula is still under team discussion.  Only this file needs
to change if the formula is revised.

Owner: Knsrikanta (ML Inference)
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from config import (
    SCORE_MIN,
    SCORE_MAX,
    WEIGHT_AHC,
    WEIGHT_DHC,
    WEIGHT_TCC,
)

log = logging.getLogger("atlas.ml.health_score")


# =============================================================================
# Component 1: Anomaly Health Component (AHC)
# =============================================================================

def _compute_ahc(anomaly_scores: pd.Series) -> pd.Series:
    """
    Normalise raw isolation-forest decision_function scores to [0, 100].

    The decision_function returns:
        > 0  →  more "inlier-like" (normal)
        < 0  →  more "outlier-like" (anomaly)

    Formula:
        AHC = 100 * (score - SCORE_MIN) / (SCORE_MAX - SCORE_MIN)

    Values outside [SCORE_MIN, SCORE_MAX] are clipped before normalisation
    so a pathological score never produces a component outside [0, 100].

    Args:
        anomaly_scores: Raw scores from model.decision_function().

    Returns:
        Series of AHC values in [0.0, 100.0].
    """
    score_range = SCORE_MAX - SCORE_MIN

    if score_range == 0.0:
        log.warning(
            "SCORE_MIN == SCORE_MAX (%s). AHC will be 50 for all rows. "
            "Calibrate ML_SCORE_MIN / ML_SCORE_MAX.",
            SCORE_MIN,
        )
        return pd.Series(50.0, index=anomaly_scores.index)

    clipped = anomaly_scores.clip(lower=SCORE_MIN, upper=SCORE_MAX)
    ahc = 100.0 * (clipped - SCORE_MIN) / score_range
    return ahc.clip(0.0, 100.0)


# =============================================================================
# Component 2: Deviation Health Component (DHC)
# =============================================================================

def _compute_dhc(df: pd.DataFrame, metric_col: str = "MetricValue") -> pd.Series:
    """
    Measure how far each row's metric value deviates from the batch average,
    normalised by the batch range.

    Formula:
        batch_min   = MetricValue.min()
        batch_max   = MetricValue.max()
        batch_avg   = MetricValue.mean()
        value_range = batch_max - batch_min

        deviation   = abs(current - batch_avg) / value_range
        DHC         = 100 * (1 - deviation)

    A device whose value equals the batch average gets DHC = 100.
    A device at either extreme of the range gets DHC ≈ 0 (or negative,
    which is then clamped to 0).

    Divide-by-zero guard: if value_range == 0 (all identical values),
    all DHC values are 100 (no deviation from a constant signal).

    Args:
        df:         DataFrame containing the metric column.
        metric_col: Column to compute deviation on (default: MetricValue).

    Returns:
        Series of DHC values in [0.0, 100.0].
    """
    if metric_col not in df.columns:
        log.warning(
            "Column '%s' not found for DHC calculation. Returning DHC=50.", metric_col
        )
        return pd.Series(50.0, index=df.index)

    values = df[metric_col]
    batch_min = values.min()
    batch_max = values.max()
    batch_avg = values.mean()
    value_range = batch_max - batch_min

    if value_range == 0.0:
        log.debug("MetricValue has zero range in this batch. DHC = 100 for all rows.")
        return pd.Series(100.0, index=df.index)

    deviation = (values - batch_avg).abs() / value_range
    dhc = 100.0 * (1.0 - deviation)
    return dhc.clip(0.0, 100.0)


# =============================================================================
# Component 3: Temporal Consistency Component (TCC)
# =============================================================================

def _compute_tcc(
    df: pd.DataFrame,
    historical_df: Optional[pd.DataFrame],
    metric_col: str = "MetricValue",
) -> pd.Series:
    """
    Measure how closely each device's current value matches the historical
    average for that (device_id, hour_of_day) pair.

    Formula:
        expected = historical_df.groupby(["device_id","hour_of_day"])[metric_col].mean()
        value_range = batch_max - batch_min

        TCC = 100 * (1 - abs(current - expected) / value_range)

    If no historical data is provided, or a device/hour combination is not
    found in history, TCC falls back to DHC for that row (conservative default).

    Args:
        df:             Current batch DataFrame (must have device_id, hour_of_day).
        historical_df:  Historical DataFrame for TCC lookup.  May be None.
        metric_col:     Column to compare (default: MetricValue).

    Returns:
        Series of TCC values in [0.0, 100.0].
    """
    if historical_df is None or historical_df.empty:
        log.info("No historical data provided for TCC. Using DHC as fallback.")
        return _compute_dhc(df, metric_col)

    required_cols = {"device_id", "hour_of_day", metric_col}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        log.warning(
            "TCC: missing columns %s. Falling back to DHC.", missing
        )
        return _compute_dhc(df, metric_col)

    if not required_cols.issubset(historical_df.columns):
        log.warning(
            "TCC: historical_df missing required columns. Falling back to DHC."
        )
        return _compute_dhc(df, metric_col)

    # Build lookup table: expected value per (device_id, hour_of_day)
    expected_map = (
        historical_df
        .groupby(["device_id", "hour_of_day"])[metric_col]
        .mean()
    )

    # Batch-level range for normalisation (same as DHC)
    batch_min = df[metric_col].min()
    batch_max = df[metric_col].max()
    value_range = batch_max - batch_min

    tcc_values: list[float] = []

    for idx, row in df.iterrows():
        key = (row["device_id"], row["hour_of_day"])
        if key in expected_map.index:
            expected = expected_map[key]
            if value_range == 0.0:
                tcc_values.append(100.0)
            else:
                deviation = abs(row[metric_col] - expected) / value_range
                tcc = 100.0 * (1.0 - deviation)
                tcc_values.append(float(np.clip(tcc, 0.0, 100.0)))
        else:
            # Unknown device/hour — use DHC for this row as a safe fallback
            if value_range == 0.0:
                tcc_values.append(100.0)
            else:
                batch_avg = df[metric_col].mean()
                deviation = abs(row[metric_col] - batch_avg) / value_range
                tcc = 100.0 * (1.0 - deviation)
                tcc_values.append(float(np.clip(tcc, 0.0, 100.0)))

    return pd.Series(tcc_values, index=df.index)


# =============================================================================
# Public Interface
# =============================================================================

def calculate_health_score(
    df: pd.DataFrame,
    anomaly_scores: pd.Series,
    historical_df: Optional[pd.DataFrame] = None,
    metric_col: str = "MetricValue",
) -> pd.Series:
    """
    Compute a composite health score for each row in df.

    This is the ONLY function the rest of the pipeline calls.
    If the team decides to change the formula, only this file needs
    to be modified.

    Formula (provisional — subject to team decision):
        health_score = W1*AHC + W2*DHC + W3*TCC
        clamped to [0, 100]

    Health score classification:
        90 – 100  →  Healthy
        70 –  89  →  Warning
        50 –  69  →  Degraded
         0 –  49  →  Critical

    Args:
        df:             Current-batch DataFrame (must contain MetricValue,
                        device_id, hour_of_day, day_of_week).
        anomaly_scores: Raw scores from model.decision_function() — same
                        index as df.
        historical_df:  Optional historical DataFrame for TCC calculation.
                        When None, TCC falls back to DHC.
        metric_col:     Primary metric column (default: MetricValue).

    Returns:
        Series of health scores in [0.0, 100.0], same index as df.
    """
    log.debug("Computing AHC...")
    ahc = _compute_ahc(anomaly_scores)

    log.debug("Computing DHC...")
    dhc = _compute_dhc(df, metric_col)

    log.debug("Computing TCC...")
    tcc = _compute_tcc(df, historical_df, metric_col)

    # Weighted combination
    health = WEIGHT_AHC * ahc + WEIGHT_DHC * dhc + WEIGHT_TCC * tcc

    # Final clamp — guarantee output is always in [0, 100]
    health = health.clip(0.0, 100.0)

    # Summary statistics for logging
    log.info(
        "Health score summary | mean=%.1f | min=%.1f | max=%.1f | "
        "critical=%d | degraded=%d | warning=%d | healthy=%d",
        health.mean(),
        health.min(),
        health.max(),
        int((health < 50).sum()),
        int(((health >= 50) & (health < 70)).sum()),
        int(((health >= 70) & (health < 90)).sum()),
        int((health >= 90).sum()),
    )

    return health
