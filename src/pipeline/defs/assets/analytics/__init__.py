# defs/assets/analytics/__init__.py
"""
Analytics assets for operator risk scoring.

Asset dependency flow:
    snapshots → volatility_metrics, concentration_metrics
              → performance_scores, economic_scores, network_scores
              → operator_analytics (final)
"""

from .volatility import volatility_metrics_asset
from .concentration import concentration_metrics_asset
from .performance import performance_scores_asset
from .economic import economic_scores_asset
from .network import network_scores_asset
from .risk_scores import operator_analytics_asset

__all__ = [
    "volatility_metrics_asset",
    "concentration_metrics_asset",
    "performance_scores_asset",
    "economic_scores_asset",
    "network_scores_asset",
    "operator_analytics_asset",
]
