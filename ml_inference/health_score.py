"""
ATLAS ML Inference Pipeline — Health Score Calculator
======================================================
This module is intentionally isolated from the rest of the inference
pipeline so that the health-score formula can be changed by the team
without touching inference.py.

Three components are combined into a single score in [0, 100]:

    AHC  — Anomaly Health Component
           Normalised isolation-forest anomaly score using trained
           min/max bounds from health_score_config.pkl.
           High AHC  → model considers device "normal".

    DHC  — Deviation Health Component
           How far the current avg_metric_value is from the batch average,
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

Owner: S Nandini (ML Inference)
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

from config import (
    SCORE_MIN_OVERRIDE,
    SCORE_MAX_OVERRIDE,
    WEIGHT_AHC,
    WEIGHT_DHC,
    WEIGHT_TCC,
)

log = logging.getLogger("atlas.ml.health_score")

# Primary metric column in the telemetry data (avg power in Watts).
# This is what DHC and TCC measure deviation/consistency against.
_METRIC_COL = "avg_metric_value"


# =============================================================================
# Component 1: Anomaly Health Component (AHC)
# =============================================================================

def _compute_ahc(
    anomaly_scores: pd.Series,
    score_min: float,
    score_max: float,
) -> pd.Series:
    """
    Normalise raw isolation-forest decision_function scores to [0, 100].

    The decision_function returns:
        > 0  →  more "inlier-like" (normal)
        < 0  →  more "outlier-like" (anomaly)

    Formula (matches Sanjula's health_score() in predict.py):
        AHC = 100 * (score - score_min) / (score_max - score_min)

    Args:
        anomaly_scores: Raw scores from model.decision_function().
        score_min: Minimum score observed during training (from health_score_config.pkl).
        score_max: Maximum score observed during training (from health_score_config.pkl).

    Returns:
        Series of AHC values in [0.0, 100.0].
    """
    # Allow env-var override for manual calibration
    if SCORE_MIN_OVERRIDE:
        score_min = float(SCORE_MIN_OVERRIDE)
    if SCORE_MAX_OVERRIDE:
        score_max = float(SCORE_MAX_OVERRIDE)

    score_range = score_max - score_min

    if score_range == 0.0:
        log.warning(
            "score_min == score_max (%.4f). AHC will be 50 for all rows. "
            "Retrain or calibrate ML_SCORE_MIN / ML_SCORE_MAX.",
            score_min,
        )
        return pd.Series(50.0, index=anomaly_scores.index)

    ahc = 100.0 * (anomaly_scores - score_min) / score_range
    return ahc.clip(0.0, 100.0)


# =============================================================================
# Component 2: Deviation Health Component (DHC)
# =============================================================================

def _compute_dhc(df: pd.DataFrame) -> pd.Series:
    """
    Measure how far each row's avg_metric_value deviates from the batch average,
    normalised by the batch range.

    Formula:
        batch_min   = avg_metric_value.min()
        batch_max   = avg_metric_value.max()
        batch_avg   = avg_metric_value.mean()
        value_range = batch_max - batch_min

        deviation   = abs(current - batch_avg) / value_range
        DHC         = 100 * (1 - deviation)

    A device whose value equals the batch average gets DHC = 100.
    A device at either extreme gets DHC ≈ 0 (clamped).

    Divide-by-zero guard: if value_range == 0, all DHC values are 100.

    Returns:
        Series of DHC values in [0.0, 100.0].
    """
    if _METRIC_COL not in df.columns:
        log.warning(
            "Column '%s' not found for DHC calculation. Returning DHC=50.", _METRIC_COL
        )
        return pd.Series(50.0, index=df.index)

    values = df[_METRIC_COL]
    batch_min = values.min()
    batch_max = values.max()
    batch_avg = values.mean()
    value_range = batch_max - batch_min

    if value_range == 0.0:
        log.debug("avg_metric_value has zero range in this batch. DHC = 100 for all rows.")
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
) -> pd.Series:
    """
    Measure how closely each device's current avg_metric_value matches the
    historical average for that (device_id, hour_of_day) pair.

    Vectorised implementation — no row-by-row Python loops.

    Formula:
        expected    = historical_df.groupby(["device_id","hour_of_day"])["avg_metric_value"].mean()
        value_range = batch_max - batch_min  (same as DHC)

        TCC = 100 * (1 - abs(current - expected) / value_range)

    Fallback rules:
        - No historical_df provided  →  TCC = DHC (conservative)
        - device/hour not in history →  TCC = DHC for that row

    Returns:
        Series of TCC values in [0.0, 100.0].
    """
    if historical_df is None or historical_df.empty:
        log.info("No historical data provided for TCC. Using DHC as fallback.")
        return _compute_dhc(df)

    required_cols = {"device_id", "hour_of_day", _METRIC_COL}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        log.warning("TCC: missing columns %s. Falling back to DHC.", missing)
        return _compute_dhc(df)

    if not required_cols.issubset(historical_df.columns):
        log.warning("TCC: historical_df missing required columns. Falling back to DHC.")
        return _compute_dhc(df)

    # Build vectorised expected-value lookup
    expected_map = (
        historical_df
        .groupby(["device_id", "hour_of_day"])[_METRIC_COL]
        .mean()
        .rename("expected_value")
    )

    # Merge current batch with expected values (vectorised join)
    df_temp = df[["device_id", "hour_of_day", _METRIC_COL]].copy()
    df_temp = df_temp.join(expected_map, on=["device_id", "hour_of_day"])

    # Batch-level range for normalisation
    batch_min = df[_METRIC_COL].min()
    batch_max = df[_METRIC_COL].max()
    value_range = batch_max - batch_min

    if value_range == 0.0:
        return pd.Series(100.0, index=df.index)

    # For rows with a known expected value, compute TCC
    deviation = (df_temp[_METRIC_COL] - df_temp["expected_value"]).abs() / value_range
    tcc = 100.0 * (1.0 - deviation)

    # For rows without a history entry (NaN expected_value), fall back to DHC
    dhc_fallback = _compute_dhc(df)
    tcc = tcc.where(df_temp["expected_value"].notna(), other=dhc_fallback)

    return tcc.clip(0.0, 100.0)


# =============================================================================
# Public Interface
# =============================================================================

def calculate_health_score(
    df: pd.DataFrame,
    anomaly_scores: pd.Series,
    score_min: float,
    score_max: float,
    historical_df: Optional[pd.DataFrame] = None,
) -> pd.Series:
    """
    Compute a composite health score for each row in df.

    This is the ONLY function the rest of the pipeline calls.
    If the team decides to change the formula, only this file needs
    to be modified.

    Formula:
        health_score = W1*AHC + W2*DHC + W3*TCC
        clamped to [0, 100]

    Health score classification:
        90 – 100  →  Healthy
        70 –  89  →  Warning
        50 –  69  →  Degraded
         0 –  49  →  Critical

    Args:
        df:             Current-batch DataFrame (must contain avg_metric_value,
                        device_id, hour_of_day after feature engineering).
        anomaly_scores: Raw scores from model.decision_function() — same
                        index as df.
        score_min:      Training-time minimum anomaly score (from health_score_config.pkl).
        score_max:      Training-time maximum anomaly score (from health_score_config.pkl).
        historical_df:  Optional historical DataFrame for TCC calculation.
                        When None, TCC falls back to DHC.

    Returns:
        Series of health scores in [0.0, 100.0], same index as df.
    """
    log.debug("Computing AHC...")
    ahc = _compute_ahc(anomaly_scores, score_min, score_max)

    log.debug("Computing DHC...")
    dhc = _compute_dhc(df)

    log.debug("Computing TCC...")
    tcc = _compute_tcc(df, historical_df)

    # Weighted combination
    health = WEIGHT_AHC * ahc + WEIGHT_DHC * dhc + WEIGHT_TCC * tcc

    # Final clamp — guarantee output is always in [0, 100]
    health = health.clip(0.0, 100.0)

    # Summary statistics for logging
    log.info(
        "Health score summary | mean=%.1f | min=%.1f | max=%.1f | "
        "AHC_mean=%.1f | DHC_mean=%.1f | TCC_mean=%.1f | "
        "critical=%d | degraded=%d | warning=%d | healthy=%d",
        health.mean(),
        health.min(),
        health.max(),
        ahc.mean(),
        dhc.mean(),
        tcc.mean(),
        int((health < 50).sum()),
        int(((health >= 50) & (health < 70)).sum()),
        int(((health >= 70) & (health < 90)).sum()),
        int((health >= 90).sum()),
    )

    return health
