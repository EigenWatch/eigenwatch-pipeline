from typing import Dict, Optional
import pandas as pd


# --- Core statistical helpers --- #
def herfindahl_hirschman_index(percentages: pd.Series) -> float:
    """HHI = sum of squared percentages (0-1 scale)."""
    return float((percentages**2).sum())


def coefficient_of_variation(values: pd.Series) -> float:
    """CV = std / mean."""
    mean = values.mean()
    return float(values.std() / mean) if mean > 0 else 0.0


def top_n_share(percentages: pd.Series, n: int) -> float:
    """Top-N concentration as a fraction (0-1)."""
    return float(percentages.sort_values(ascending=False).head(n).sum())


# --- Reusable metric calculators --- #
def compute_concentration_metrics(
    df: pd.DataFrame,
    amount_col: str,
) -> Dict:
    """
    Generic concentration metrics for any entity.
    - df: DataFrame with at least `amount_col`
    - amount_col: column holding numeric amounts (e.g., shares, tokens)
    - id_col: optional unique entity ID (not required for math)
    """
    if df.empty or df[amount_col].sum() <= 0:
        return {}

    total = df[amount_col].sum()
    percentages = df[amount_col] / total

    hhi = herfindahl_hirschman_index(percentages)
    cv = coefficient_of_variation(df[amount_col])

    return {
        "hhi_value": hhi,
        "coefficient_of_variation": cv,
        "top_1_percentage": top_n_share(percentages, 1) * 100,
        "top_5_percentage": top_n_share(percentages, 5) * 100,
        "total_entities": len(df),
        "effective_entities": 1 / hhi if hhi > 0 else len(df),
        "total_amount": total,
    }


# TODO: Revisit this implimentation, should be based on historical data.
# Also look into how this affects the overall stability of the entity.
# Is it overall stable? or just currently stable.
def compute_volatility_metrics(df: pd.DataFrame, amount_col: str) -> Dict:
    """
    Generic volatility metrics.
    """
    if df.empty:
        return {}

    if len(df) < 2:
        return {
            "volatility_7d": 0.0,
            "volatility_30d": 0.0,
            "volatility_90d": 0.0,
            "coefficient_of_variation": 0.0,
            "data_points": len(df),
        }

    cv = coefficient_of_variation(df[amount_col])

    return {
        "volatility_7d": cv,
        "volatility_30d": cv * 0.8,
        "volatility_90d": cv * 0.6,
        "coefficient_of_variation": cv,
        "data_points": len(df),
    }


# TODO: Review this, i think slashing score should be a bit more considerate of when the slashing happend.
# If it is far enough in the past it should somehow be weigthed less.
def compute_slashing_metrics(
    df: pd.DataFrame, amount_col: str, timestamp_col: Optional[str] = None
) -> Dict:
    """
    Generic slashing metrics.
    - df: events
    - amount_col: column with slashed amounts
    - timestamp_col: optional (for most recent event)
    """
    if df.empty:
        return {"slashing_count": 0, "total_slashed": 0, "slashing_score": 100}

    slashing_count = len(df)
    total_slashed = df[amount_col].apply(_parse_amount).sum()

    score = max(0, 100 - (slashing_count * 25))

    return {
        "slashing_count": slashing_count,
        "total_slashed": total_slashed,
        "slashing_score": score,
        "most_recent": df[timestamp_col].max() if timestamp_col else None,
    }


def compute_commission_metrics(
    df: pd.DataFrame,
    rate_col: str,
    timestamp_col: Optional[str] = None,
) -> Dict:
    """
    Generic commission metrics.
    - df: commission events
    - rate_col: commission rate (bips, percent, etc.)
    - timestamp_col: optional sort column
    """
    if df.empty:
        return {"current_rate": None, "commission_score": 50, "changes": 0}

    if timestamp_col:
        df = df.sort_values(timestamp_col)

    latest = df.iloc[-1][rate_col]
    rate = float(latest) if pd.notna(latest) else None

    score = _score_commission(rate) if rate is not None else 50

    return {
        "current_rate": rate,
        "commission_score": score,
        "changes": len(df),
    }


# --- Internal helpers --- #
def _parse_amount(val) -> float:
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return sum(eval(val)) if val.startswith("[") else float(val)
        except Exception:
            return 0.0
    return 0.0


# TODO: Revisit this commission scoring. It should be backed by research and community feedback.
def _score_commission(rate: float) -> int:
    """Example scoring logic, domain-agnostic."""
    if 500 <= rate <= 1500:
        return 100
    elif rate < 500:
        return 70
    elif 1500 < rate <= 2500:
        return 60
    else:
        return 25
