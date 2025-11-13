"""
Central configuration for the V6 Volatility 25 (1s) bot.

This module consolidates shared constants and environment lookups so that
strategy, data pipelines, and orchestration layers reference a single source
of truth.  All environment variables are optional here—runtime components
should still validate that required secrets exist before use.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv


load_dotenv()


# --- Paths ----------------------------------------------------------------- #
PROJECT_ROOT = Path(os.getenv("TRADING_BOT_ROOT", Path.home() / "trading_bot"))
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
EXPORTS_DIR = PROJECT_ROOT / "exports"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


# --- Market / Timeframe Settings ------------------------------------------- #
MARKET_SYMBOL = os.getenv("MARKET_SYMBOL", "1HZ25V")
MARKET_NAME = os.getenv("MARKET_NAME", "Volatility 25 (1s) Index")

TIMEFRAME_SECONDS: Dict[str, int] = {
    "4H": 4 * 60 * 60,
    "1H": 60 * 60,
    "30M": 30 * 60,
    "15M": 15 * 60,
    "5M": 5 * 60,
    "1M": 60,
}


# --- Strategy Parameters --------------------------------------------------- #
EMA_TREND_H4 = int(os.getenv("EMA_TREND_H4", 50))
EMA_TREND_H1 = int(os.getenv("EMA_TREND_H1", 50))
EMA_FAST = int(os.getenv("EMA_FAST", 10))
EMA_SLOW = int(os.getenv("EMA_SLOW", 25))
FLAT_SLOPE_THRESHOLD = float(os.getenv("FLAT_SLOPE_THRESHOLD", 0.00005))

ZONE_BUFFER = float(os.getenv("ZONE_BUFFER", 0.0015))
ZONE_MERGE_DISTANCE = float(os.getenv("ZONE_MERGE_DISTANCE", 0.001))
MIN_RISK_REWARD = float(os.getenv("MIN_RISK_REWARD", 1.5))
MAX_TRADES_PER_SESSION = int(os.getenv("MAX_TRADES_PER_SESSION", 10))

LAYER_COUNT = int(os.getenv("LAYER_COUNT", 5))
TOTAL_STAKE = float(os.getenv("TOTAL_STAKE", 10.0))
LAYER_DELAY_SECONDS = float(os.getenv("LAYER_DELAY_SECONDS", 0.2))
DERIV_MULTIPLIER = int(os.getenv("DERIV_MULTIPLIER", 100))
HARD_STOP_PER_LAYER = float(os.getenv("HARD_STOP_PER_LAYER", 0.40))

TRAILING_ACTIVATION = float(os.getenv("TRAILING_ACTIVATION", 5.0))
TRAILING_GAP = float(os.getenv("TRAILING_GAP", 2.0))

MAX_ZONE_EXPORTS = int(os.getenv("MAX_ZONE_EXPORTS", 15))


# --- Session Targets ------------------------------------------------------- #
DAILY_TARGET = float(os.getenv("DAILY_TARGET", 50.0))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", -30.0))
MAX_SESSION_DURATION = timedelta(
    hours=int(os.getenv("MAX_SESSION_HOURS", 12))
)

# --- Bonus Tier Settings --------------------------------------------------- #
BONUS_TARGET = float(os.getenv("BONUS_TARGET", 0.0))
BONUS_MIN_RECENT_TRADES = int(os.getenv("BONUS_MIN_RECENT_TRADES", 3))
BONUS_MIN_AVG_SCORE = float(os.getenv("BONUS_MIN_AVG_SCORE", 0.8))
BONUS_MIN_WIN_RATE = float(os.getenv("BONUS_MIN_WIN_RATE", 0.6))


# --- Email Subjects / Templates ------------------------------------------- #
EMAIL_TRADE_OPEN_SUBJECT = os.getenv("EMAIL_TRADE_OPEN_SUBJECT", "Trade Opened")
EMAIL_TRADE_CLOSE_SUBJECT = os.getenv("EMAIL_TRADE_CLOSE_SUBJECT", "Trades Closed")
EMAIL_TARGET_HIT_SUBJECT = os.getenv("EMAIL_TARGET_HIT_SUBJECT", "Daily Target Hit")
EMAIL_MAX_LOSS_SUBJECT = os.getenv("EMAIL_MAX_LOSS_SUBJECT", "Daily Loss Limit Reached")


# --- Deriv API ------------------------------------------------------------- #
DERIV_APP_ID = os.getenv("DERIV_APP_ID")
DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN")


# --- Utility Data Classes -------------------------------------------------- #
@dataclass
class TimeframeConfig:
    trend_higher: str = "4H"
    trend_mid: str = "1H"
    zone_timeframes: Optional[List[str]] = None
    entry_timeframe: str = "5M"

    def __post_init__(self) -> None:
        if self.zone_timeframes is None:
            self.zone_timeframes = ["1H", "30M"]


TIMEFRAMES = TimeframeConfig()


__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "EXPORTS_DIR",
    "LOG_DIR",
    "MARKET_SYMBOL",
    "MARKET_NAME",
    "TIMEFRAME_SECONDS",
    "EMA_TREND_H4",
    "EMA_TREND_H1",
    "EMA_FAST",
    "EMA_SLOW",
    "FLAT_SLOPE_THRESHOLD",
    "ZONE_BUFFER",
    "ZONE_MERGE_DISTANCE",
    "MIN_RISK_REWARD",
    "MAX_TRADES_PER_SESSION",
    "LAYER_COUNT",
    "TOTAL_STAKE",
    "LAYER_DELAY_SECONDS",
    "DERIV_MULTIPLIER",
    "HARD_STOP_PER_LAYER",
    "TRAILING_ACTIVATION",
    "TRAILING_GAP",
    "MAX_ZONE_EXPORTS",
    "DAILY_TARGET",
    "MAX_DAILY_LOSS",
    "MAX_SESSION_DURATION",
    "BONUS_TARGET",
    "BONUS_MIN_RECENT_TRADES",
    "BONUS_MIN_AVG_SCORE",
    "BONUS_MIN_WIN_RATE",
    "EMAIL_TRADE_OPEN_SUBJECT",
    "EMAIL_TRADE_CLOSE_SUBJECT",
    "EMAIL_TARGET_HIT_SUBJECT",
    "EMAIL_MAX_LOSS_SUBJECT",
    "DERIV_APP_ID",
    "DERIV_API_TOKEN",
    "TIMEFRAMES",
]

