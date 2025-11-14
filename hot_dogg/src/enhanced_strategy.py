from __future__ import annotations

import asyncio
import csv
import json
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import uuid

import pandas as pd
from deriv_api import DerivAPI

from config.settings import (
    DERIV_API_TOKEN,
    DERIV_APP_ID,
    DERIV_MULTIPLIER,
    EMA_FAST,
    EMA_SLOW,
    EMA_TREND_H1,
    EMA_TREND_H4,
    EXPORTS_DIR,
    HARD_STOP_PER_LAYER,
    LAYER_COUNT,
    LAYER_DELAY_SECONDS,
    MAX_TRADES_PER_SESSION,
    MAX_ZONE_EXPORTS,
    MIN_RISK_REWARD,
    TIMEFRAME_SECONDS,
    TIMEFRAMES,
    TOTAL_STAKE,
    ZONE_BUFFER,
    ZONE_MERGE_DISTANCE,
    FLAT_SLOPE_THRESHOLD,
)

# --- Constants for Zone Types ---
ZONE_OB_BULLISH = "Bullish OB"
ZONE_OB_BEARISH = "Bearish OB"
ZONE_FVG_BULLISH = "Bullish FVG"
ZONE_FVG_BEARISH = "Bearish FVG"


@dataclass
class TimeframeRequest:
    name: str
    granularity: int
    count: int


@dataclass
class EntryCandles:
    previous: pd.Series
    pattern: pd.Series
    confirmation: pd.Series

    @property
    def price(self) -> float:
        return float(self.confirmation['close'])

class EnhancedZoneStrategy:
    """
    PROFESSIONAL V6.0 (V25-1s Sniper)
    - Trend: Strict Slope Detection (No Ranging).
    - Entry: 5-Layer DCA Attack via Deriv API.
    - Exit: Elastic Band Trailing Profit.
    """

    def __init__(
        self,
        logger=None,
        ml_model_path: Optional[Path] = None,
    ) -> None:
        self.logger = logger

        # --- Memory / counters --------------------------------------------- #
        self.highest_session_profit = 0.0
        self._signal_counter = 0
        self._last_signal_id: Optional[str] = None

        # --- Timeframes ----------------------------------------------------- #
        self.trend_tf_h4 = TIMEFRAMES.trend_higher
        self.trend_tf_h1 = TIMEFRAMES.trend_mid
        self.zone_timeframes = TIMEFRAMES.zone_timeframes
        self.entry_timeframe = TIMEFRAMES.entry_timeframe

        # --- Indicators ----------------------------------------------------- #
        self.ema_trend_h4 = EMA_TREND_H4
        self.ema_trend_h1 = EMA_TREND_H1
        self.ema_fast = EMA_FAST
        self.ema_slow = EMA_SLOW

        # --- Buffers / thresholds ------------------------------------------- #
        self.zone_buffer = ZONE_BUFFER
        self.zone_merge_distance = ZONE_MERGE_DISTANCE
        self.min_risk_reward = MIN_RISK_REWARD
        self.max_trades_per_session = MAX_TRADES_PER_SESSION

        # --- Paths ---------------------------------------------------------- #
        default_model_path = EXPORTS_DIR.parent / "ml_trade_model.pkl"
        self.ml_model_path = Path(ml_model_path or default_model_path)

        if self.logger:
            self.logger.info("EnhancedZoneStrategy (V6.0 Auto-Layering) initialized.")

    def _build_timeframe_requests(self) -> List[TimeframeRequest]:
        ordered = [
            self.trend_tf_h4,
            self.trend_tf_h1,
            *self.zone_timeframes,
            self.entry_timeframe,
        ]
        requests: List[TimeframeRequest] = []
        seen = set()
        for tf in ordered:
            if tf in seen:
                continue
            seen.add(tf)
            granularity = TIMEFRAME_SECONDS.get(tf)
            if granularity is None:
                if self.logger:
                    self.logger.warning(f"Unsupported timeframe requested: {tf}")
                continue
            requests.append(TimeframeRequest(tf, granularity, self._timeframe_lookback(tf)))
        return requests

    def _timeframe_lookback(self, timeframe: str) -> int:
        if timeframe == self.trend_tf_h4:
            return max(self.ema_trend_h4 + 50, 120)
        if timeframe == self.trend_tf_h1:
            return max(self.ema_trend_h1 + 50, 200)
        return 150

    def _collect_timeframe_results(
        self,
        requests: List[TimeframeRequest],
        results: List[Any],
    ) -> Optional[Dict[str, pd.DataFrame]]:
        data: Dict[str, pd.DataFrame] = {}
        for request, result in zip(requests, results):
            if isinstance(result, Exception) or result is None:
                if self.logger:
                    self.logger.error(f"Error fetching {request.name} data: {result}")
                return None
            if len(result) < 30:
                if self.logger:
                    self.logger.warning(f"Insufficient data for timeframe {request.name}")
                return None
            data[request.name] = result
        return data

    # --- Data Fetching ------------------------------------------------------ #
    async def fetch_all_timeframe_data(
        self,
        symbol: str,
        data_source,
    ) -> Optional[Dict[str, pd.DataFrame]]:
        try:
            requests = self._build_timeframe_requests()
            if not requests:
                return None

            if self.logger:
                self.logger.debug(
                    "Preparing OHLC requests for %s: %s",
                    symbol,
                    ", ".join(f"{req.name}({req.count})" for req in requests),
                )

            coroutines = [
                data_source.get_ohlc_data(symbol, req.granularity, req.count)
                for req in requests
            ]
            results = await asyncio.gather(*coroutines, return_exceptions=True)

            if self.logger:
                successful = sum(
                    1 for result in results if not isinstance(result, Exception) and result is not None
                )
                self.logger.debug(
                    "Timeframe fetch complete for %s: %d/%d successful",
                    symbol,
                    successful,
                    len(requests),
                )

            return self._collect_timeframe_results(requests, results)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Fetch error: {e}")
            return None

    # --- STRICT TREND DETECTION (Slope Filter) ------------------------------ #
    def _get_ema_trend(
        self,
        df: pd.DataFrame,
        ema_period: int,
        tf_name: str,
    ) -> str:
        if len(df) < ema_period + 5:
            return "range"
        try:
            ema = df["close"].ewm(span=ema_period, adjust=False).mean()
            curr_ema = ema.iloc[-1]
            prev_ema = ema.iloc[-3]

            # Calculate Slope
            slope = (curr_ema - prev_ema) / prev_ema if prev_ema != 0 else 0

            # Flat Filter (Avoid Ranges)
            if abs(slope) < FLAT_SLOPE_THRESHOLD:
                if self.logger:
                    self.logger.info(
                        f"Trend {tf_name}: RANGE (Flat Slope: {slope:.7f})"
                    )
                return "range"

            close_price = df["close"].iloc[-1]
            if curr_ema > prev_ema and close_price > curr_ema:
                return "bullish"
            if curr_ema < prev_ema and close_price < curr_ema:
                return "bearish"
            return "range"
        except Exception:
            return "range"

    def get_h4_trend(self, df_h4: pd.DataFrame) -> str:
        return self._get_ema_trend(df_h4, self.ema_trend_h4, "H4")

    def get_h1_trend(self, df_h1: pd.DataFrame) -> str:
        return self._get_ema_trend(df_h1, self.ema_trend_h1, "H1")

    def calculate_fibonacci_levels(self, df: pd.DataFrame) -> dict:
        lookback_df = df.tail(100)
        if lookback_df.empty:
            return {}
        high = lookback_df["high"].max()
        low = lookback_df["low"].min()
        diff = high - low
        if diff == 0:
            return {}
        return {
            "0.0": high,
            "50.0": high - diff * 0.5,
            "61.8": high - diff * 0.618,
            "100.0": low,
        }

    def is_bullish_engulfing(self, prev, curr) -> bool:
        return (
            prev["close"] < prev["open"]
            and curr["close"] > curr["open"]
            and curr["open"] <= prev["close"]
            and curr["close"] >= prev["open"]
        )

    def is_bearish_engulfing(self, prev, curr) -> bool:
        return (
            prev["close"] > prev["open"]
            and curr["close"] < curr["open"]
            and curr["open"] >= prev["close"]
            and curr["close"] <= prev["open"]
        )

    def is_hammer(self, candle) -> bool:
        body = abs(candle["close"] - candle["open"])
        return body > 0 and (min(candle["open"], candle["close"]) - candle["low"]) > 1.8 * body

    def ema_crossover(self, df: pd.DataFrame) -> str:
        if len(df) < self.ema_slow:
            return "neutral"
        ema_fast = df["close"].ewm(span=self.ema_fast, adjust=False).mean().iloc[-1]
        ema_slow = df["close"].ewm(span=self.ema_slow, adjust=False).mean().iloc[-1]
        if ema_fast > ema_slow:
            return "bullish"
        if ema_fast < ema_slow:
            return "bearish"
        return "neutral"

    def check_pattern_confluence(self, prev, curr, zone_type: str) -> bool:
        if zone_type == 'demand':
            return (
                self.is_bullish_engulfing(prev, curr)
                or self.is_hammer(curr)
                or curr['close'] > prev['high']
            )
        return (
            self.is_bearish_engulfing(prev, curr)
            or curr['close'] < prev['low']
        )

    def _is_bearish_order_block(
        self,
        prev: pd.Series,
        curr: pd.Series,
        next_c: pd.Series,
        avg_body_value: float,
    ) -> bool:
        return (
            prev['close'] > prev['open']
            and curr['close'] < curr['open']
            and curr['low'] < prev['low']
            and curr['body_size'] > avg_body_value
            and next_c['close'] < curr['close']
        )

    def _is_bullish_order_block(
        self,
        prev: pd.Series,
        curr: pd.Series,
        next_c: pd.Series,
        avg_body_value: float,
    ) -> bool:
        return (
            prev['close'] < prev['open']
            and curr['close'] > curr['open']
            and curr['high'] > prev['high']
            and curr['body_size'] > avg_body_value
            and next_c['close'] > curr['close']
        )

    def _create_block(
        self,
        block_type: str,
        price: float,
        low: float,
        high: float,
        timeframe: str,
        strength: float,
    ) -> Dict[str, Any]:
        return {
            "type": block_type,
            "price": price,
            "low": low,
            "high": high,
            "timeframe": timeframe,
            "strength": strength,
        }

    def _identify_order_block(
        self,
        prev: pd.Series,
        curr: pd.Series,
        next_c: pd.Series,
        timeframe: str,
        avg_body_value: float,
    ) -> Optional[Dict[str, Any]]:
        if pd.isna(avg_body_value) or avg_body_value == 0:
            return None

        body_ratio = curr["body_size"] / avg_body_value if avg_body_value else 0
        strength = round(min(body_ratio, 3.0), 3)

        if self._is_bearish_order_block(prev, curr, next_c, avg_body_value):
            return self._create_block(
                ZONE_OB_BEARISH,
                price=prev['high'],
                low=prev['low'],
                high=prev['high'],
                timeframe=timeframe,
                strength=strength,
            )
        if self._is_bullish_order_block(prev, curr, next_c, avg_body_value):
            return self._create_block(
                ZONE_OB_BULLISH,
                price=prev['low'],
                low=prev['low'],
                high=prev['high'],
                timeframe=timeframe,
                strength=strength,
            )
        return None

    def _passes_entry_filters(
        self,
        df_entry: pd.DataFrame,
        trades_this_session: int,
        structure: dict,
        h4_trend: str,
        h1_trend: str,
    ) -> bool:
        if self.logger:
            self.logger.debug(
                "Entry filter check -> trades=%d len(df)=%d structure=%s H4=%s H1=%s",
                trades_this_session,
                len(df_entry) if df_entry is not None else 0,
                structure.get('trend') if structure else None,
                h4_trend,
                h1_trend,
            )
        if trades_this_session >= self.max_trades_per_session:
            return False
        if len(df_entry) < 3:
            return False
        if structure and structure.get('trend') == 'range':
            return False
        if h4_trend == 'range' or h1_trend == 'range':
            return False
        if h4_trend != h1_trend:
            return False
        return True

    def _prepare_entry_candles(self, df_entry: pd.DataFrame) -> EntryCandles:
        return EntryCandles(
            previous=df_entry.iloc[-3],
            pattern=df_entry.iloc[-2],
            confirmation=df_entry.iloc[-1],
        )

    def _fib_guard_allows_entry(self, trend: str, price: float, fib_mid: Optional[float]) -> bool:
        if fib_mid is None:
            return True
        if trend == 'bullish':
            return price >= fib_mid
        if trend == 'bearish':
            return price <= fib_mid
        return True

    def _split_zones(self, all_zones: List[dict]) -> Tuple[List[dict], List[dict]]:
        demand = [z for z in all_zones if ZONE_OB_BULLISH in z['type'] or ZONE_FVG_BULLISH in z['type']]
        supply = [z for z in all_zones if ZONE_OB_BEARISH in z['type'] or ZONE_FVG_BEARISH in z['type']]
        return demand, supply

    def _resolve_trade_direction(
        self,
        trend: str,
        demand: List[dict],
        supply: List[dict],
    ) -> Optional[Tuple[str, List[dict]]]:
        if trend == 'bullish':
            return 'LONG', demand
        if trend == 'bearish':
            return 'SHORT', supply
        return None

    def _zone_matches_pattern(self, zone: dict, pattern_candle: pd.Series) -> bool:
        return (
            pattern_candle['low'] <= zone['high'] * (1 + self.zone_buffer)
            and pattern_candle['high'] >= zone['low'] * (1 - self.zone_buffer)
        )

    def _pattern_supports_zone(self, candles: EntryCandles, zone_side: str) -> bool:
        return self.check_pattern_confluence(candles.previous, candles.pattern, zone_side)

    def _ema_supports_direction(self, direction: str, ema_bias: str) -> bool:
        expected = 'bullish' if direction == 'LONG' else 'bearish'
        return ema_bias == expected

    def _confirm_breakout(self, direction: str, candles: EntryCandles) -> bool:
        if direction == 'LONG':
            return candles.confirmation['close'] > candles.pattern['high']
        return candles.confirmation['close'] < candles.pattern['low']

    def _risk_profile(self, direction: str, candles: EntryCandles, zone: dict) -> Tuple[float, float, float]:
        if direction == 'LONG':
            stop_loss = zone['low']
            risk = max(candles.price - stop_loss, 1e-5)
            take_profit = candles.price + risk * max(self.min_risk_reward, 1.0)
            rr = (take_profit - candles.price) / risk if risk else self.min_risk_reward
        else:
            stop_loss = zone['high']
            risk = max(stop_loss - candles.price, 1e-5)
            take_profit = candles.price - risk * max(self.min_risk_reward, 1.0)
            rr = (candles.price - take_profit) / risk if risk else self.min_risk_reward
        return stop_loss, take_profit, round(rr, 3)

    def _compose_signal(
        self,
        direction: str,
        candles: EntryCandles,
        zone: dict,
        stop_loss: float,
        take_profit: float,
        rr: float,
    ) -> dict:
        return {
            'signal': direction,
            'price': candles.price,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'zone_type': zone['type'],
            'zone_price': zone['price'],
            'zone_strength': zone.get('strength', 1.0),
            'pattern': 'trend_continuation',
            'risk_reward': rr,
            'trend_alignment': 1,
            'is_range_market': 0,
        }

    def _find_signal_for_direction(
        self,
        direction: str,
        zones: List[dict],
        candles: EntryCandles,
        ema_bias: str,
    ) -> Optional[dict]:
        if not self._ema_supports_direction(direction, ema_bias):
            return None

        zone_side = 'demand' if direction == 'LONG' else 'supply'
        for zone in zones:
            if not self._zone_matches_pattern(zone, candles.pattern):
                continue
            if not self._pattern_supports_zone(candles, zone_side):
                continue
            if not self._confirm_breakout(direction, candles):
                continue
            stop_loss, take_profit, rr = self._risk_profile(direction, candles, zone)
            return self._compose_signal(direction, candles, zone, stop_loss, take_profit, rr)
        return None

    # --- Zone Detection ---
    def detect_order_blocks(
        self,
        df: pd.DataFrame,
        timeframe: str,
        lookback: int = 10,
    ) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        working_df = df.copy()
        working_df["body_size"] = abs(working_df["close"] - working_df["open"])
        avg_body = working_df["body_size"].rolling(
            window=lookback,
            min_periods=max(2, lookback // 2),
        ).mean()
        for i in range(lookback, len(working_df) - 1):
            prev = working_df.iloc[i - 1]
            curr = working_df.iloc[i]
            next_c = working_df.iloc[i + 1]
            block = self._identify_order_block(prev, curr, next_c, timeframe, avg_body.iloc[i])
            if block:
                blocks.append(block)
        return blocks

    def detect_fair_value_gaps(
        self,
        df: pd.DataFrame,
        timeframe: str,
    ) -> List[Dict[str, Any]]:
        fvgs: List[Dict[str, Any]] = []
        for i in range(len(df) - 2):
            c1 = df.iloc[i]
            c3 = df.iloc[i + 2]
            gap_size = (c3["low"] - c1["high"]) if c1["high"] < c3["low"] else (
                c1["low"] - c3["high"]
            )
            if c1["high"] < c3["low"]:
                fvgs.append(
                    {
                        "type": ZONE_FVG_BULLISH,
                        "price": c3["low"],
                        "low": c1["high"],
                        "high": c3["low"],
                        "timeframe": timeframe,
                        "strength": round(max(gap_size, 0.0), 5),
                    }
                )
            elif c1["low"] > c3["high"]:
                fvgs.append(
                    {
                        "type": ZONE_FVG_BEARISH,
                        "price": c3["high"],
                        "low": c3["high"],
                        "high": c1["low"],
                        "timeframe": timeframe,
                        "strength": round(max(gap_size, 0.0), 5),
                    }
                )
        return fvgs

    # --- Entry Check -------------------------------------------------------- #
    def check_entry(
        self,
        df_entry: pd.DataFrame,
        all_zones: List[dict],
        fib_levels: dict,
        structure: dict,
        trades_this_session: int,
        h4_trend: str,
        h1_trend: str,
    ) -> Optional[dict]:
        logger = self.logger
        if not self._passes_entry_filters(df_entry, trades_this_session, structure, h4_trend, h1_trend):
            logger and logger.debug("Entry rejected by preliminary filters.")
            return None

        candles = self._prepare_entry_candles(df_entry)
        fib_mid = fib_levels.get('50.0') if fib_levels else None
        if not self._fib_guard_allows_entry(h4_trend, candles.price, fib_mid):
            logger and logger.debug(
                "Fibonacci guard blocked entry: trend=%s price=%.5f fib_mid=%s",
                h4_trend,
                candles.price,
                f"{fib_mid:.5f}" if fib_mid else None,
            )
            return None

        ema_bias = self.ema_crossover(df_entry)
        demand, supply = self._split_zones(all_zones)

        logger and logger.debug(
            "EMA bias=%s | demand zones=%d | supply zones=%d",
            ema_bias,
            len(demand),
            len(supply),
        )

        direction = self._resolve_trade_direction(h4_trend, demand, supply)

        if direction is None:
            logger and logger.debug("Higher timeframe trend neutral; skipping entry evaluation.")
            return None

        trade_direction, candidate_zones = direction
        signal = self._find_signal_for_direction(trade_direction, candidate_zones, candles, ema_bias)

        if signal is None:
            logger and logger.debug("No qualifying signal identified after zone evaluation.")
        return signal

    # --- 5-LAYER EXECUTION ---
    def _resolve_credentials(self, data_source) -> Tuple[Optional[str], Optional[str]]:
        api_token = getattr(data_source, 'deriv_api_token', None) or DERIV_API_TOKEN
        app_id = getattr(data_source, 'deriv_app_id', None) or DERIV_APP_ID
        return api_token, app_id

    def _resolve_symbol(self, signal: dict, data_source) -> str:
        return (
            getattr(data_source, 'symbol', None)
            or getattr(data_source, 'market_symbol', None)
            or signal.get('symbol', '1HZ25V')
        )

    def _log_error(self, message: str) -> None:
        if self.logger:
            self.logger.error(message)

    def _prepare_trade_context(
        self,
        signal: dict,
        data_source,
    ) -> Optional[Dict[str, Any]]:
        contract_map = {'LONG': 'MULTUP', 'SHORT': 'MULTDOWN'}
        direction = signal.get('signal')
        contract = contract_map.get(direction)
        if not contract:
            if self.logger:
                self.logger.error(f"Unsupported signal direction for trade execution: {direction}")
            return None

        stake_per_layer = self._resolve_stake_per_layer(data_source)
        if not stake_per_layer:
            if self.logger:
                self.logger.error("Unable to resolve stake per layer; skipping trade execution.")
            return None

        symbol = self._resolve_symbol(signal, data_source)
        tp_amt, sl_amt = self._calculate_order_limits(signal, stake_per_layer)

        return {
            'contract': contract,
            'stake_per_layer': stake_per_layer,
            'symbol': symbol,
            'take_profit': tp_amt,
            'stop_loss': sl_amt,
        }

    def _calculate_order_limits(self, signal: dict, stake_per_layer: float) -> Tuple[float, float]:
        pct_diff = abs(signal['take_profit'] - signal['price']) / max(signal['price'], 1e-5)
        tp_amt = round(stake_per_layer * DERIV_MULTIPLIER * pct_diff, 2)
        sl_amt = round(stake_per_layer * HARD_STOP_PER_LAYER, 2)
        return tp_amt, sl_amt

    def _coerce_positive_float(self, value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if numeric > 0 else None

    def _resolve_stake_per_layer(self, data_source) -> Optional[float]:
        stake = self._coerce_positive_float(getattr(data_source, 'stake_per_layer', None))
        if stake:
            return stake

        total_stake = self._coerce_positive_float(getattr(data_source, 'total_stake', None))
        if not total_stake:
            total_stake = self._coerce_positive_float(getattr(data_source, 'stake', None))
        if not total_stake:
            total_stake = self._coerce_positive_float(TOTAL_STAKE)

        if not total_stake or LAYER_COUNT <= 0:
            return None
        stake_per_layer = round(total_stake / float(LAYER_COUNT), 2)
        return stake_per_layer if stake_per_layer > 0 else None

    async def _authorize_api(self, app_id: int, api_token: str) -> DerivAPI:
        api = DerivAPI(app_id=int(app_id))
        await api.authorize(api_token)
        return api

    async def _request_proposal(
        self,
        api: DerivAPI,
        stake_per_layer: float,
        contract: str,
        symbol: str,
        tp_amt: float,
        sl_amt: float,
    ) -> Optional[dict]:
        proposal_request = {
            "proposal": 1,
            "amount": stake_per_layer,
            "basis": "stake",
            "contract_type": contract,
            "currency": "USD",
            "symbol": symbol,
            "multiplier": DERIV_MULTIPLIER,
            "limit_order": {"take_profit": tp_amt, "stop_loss": sl_amt},
        }
        prop = await api.proposal(proposal_request)
        if not prop or 'error' in prop:
            if self.logger:
                self.logger.error(f"Proposal error: {prop.get('error') if prop else 'unknown'}")
            return None
        return prop

    async def _open_layer(self, api: DerivAPI, proposal_id: str, stake_per_layer: float) -> Dict[str, Any]:
        buy = await api.buy({"buy": proposal_id, "price": stake_per_layer + 10})
        opened = 1 if 'buy' in buy else 0
        return {'opened': opened, 'response': buy}

    def _extract_proposal_id(self, proposal: Dict[str, Any]) -> Optional[str]:
        if not proposal:
            return None
        candidate_keys = [
            ('proposal', 'id'),
            ('proposal', 'proposal_id'),
            ('proposal', 'buy'),
            (None, 'proposal_id'),
            (None, 'buy'),
        ]
        for parent, key in candidate_keys:
            if parent:
                parent_value = proposal.get(parent, {})
                if isinstance(parent_value, dict) and parent_value.get(key):
                    return parent_value.get(key)
            else:
                if proposal.get(key):
                    return proposal.get(key)
        return None

    async def _execute_trade(
        self,
        signal: dict,
        data_source,
    ) -> Optional[Dict[str, Any]]:
        api_token, app_id = self._resolve_credentials(data_source)
        if not (api_token and app_id):
            self._log_error("Missing Deriv credentials; skipping trade execution.")
            return None

        context = self._prepare_trade_context(signal, data_source)
        if not context:
            return None

        return await self._execute_with_api(app_id, api_token, context, signal, data_source)

    async def _safe_authorize(self, app_id: int, api_token: str) -> Optional[DerivAPI]:
        try:
            return await self._authorize_api(app_id, api_token)
        except Exception as exc:
            self._log_error(f"Authorization failed: {exc}")
            return None

    async def _execute_with_api(
        self,
        app_id: int,
        api_token: str,
        context: Dict[str, Any],
        signal: Dict[str, Any],
        data_source,
    ) -> Optional[Dict[str, Any]]:
        api = await self._safe_authorize(app_id, api_token)
        if not api:
            return None

        try:
            return await self._execute_layers(api, context, signal, data_source)
        except Exception as exc:
            self._log_error(f"Trade execution failed: {exc}")
            return None
        finally:
            with suppress(Exception):
                await api.disconnect()

    async def _execute_layers(
        self,
        api: DerivAPI,
        context: Dict[str, Any],
        signal: Dict[str, Any],
        data_source,
    ) -> Optional[Dict[str, Any]]:
        opened_layers = 0
        failed_layers = 0
        layer_results: List[Dict[str, Any]] = []

        for layer_index in range(LAYER_COUNT):
            proposal = await self._request_proposal(
                api,
                stake_per_layer=context['stake_per_layer'],
                contract=context['contract'],
                symbol=context['symbol'],
                tp_amt=context['take_profit'],
                sl_amt=context['stop_loss'],
            )
            proposal_id = self._extract_proposal_id(proposal)
            if not proposal_id:
                failed_layers += 1
                self._log_error(f"Layer {layer_index + 1}: Proposal response missing identifier.")
                await asyncio.sleep(LAYER_DELAY_SECONDS)
                continue

            buy_response = await self._open_layer(api, proposal_id, context['stake_per_layer'])
            opened_layers += buy_response.get('opened', 0)
            if buy_response.get('opened', 0) == 0:
                failed_layers += 1
            layer_results.append({'proposal_id': proposal_id, **buy_response})
            await asyncio.sleep(LAYER_DELAY_SECONDS)

        self._handle_successful_layers(opened_layers, data_source, signal)

        return {
            'opened_layers': opened_layers,
            'failed_layers': failed_layers,
            'stake_per_layer': context['stake_per_layer'],
            'take_profit': context['take_profit'],
            'stop_loss': context['stop_loss'],
            'contract_type': context['contract'],
            'symbol': context['symbol'],
            'layer_results': layer_results,
        }

    def _handle_successful_layers(self, opened: int, data_source, signal: dict) -> None:
        if opened <= 0:
            return
        msg = f"✅ {opened}/{LAYER_COUNT} Layers Opened."
        if self.logger:
            self.logger.info(msg)
        if hasattr(data_source, 'send_email'):
            email_subject = f"Trade Open: {signal.get('signal')}"
            email_body = (
                f"{msg}\n"
                f"Symbol: {signal.get('symbol', data_source.symbol)}\n"
                f"Price: {signal.get('price')}\n"
                f"Stop Loss: {signal.get('stop_loss')}\n"
                f"Take Profit: {signal.get('take_profit')}\n"
                f"Zone: {signal.get('zone_type')} ({signal.get('zone_strength')})\n"
                f"Risk/Reward: {signal.get('risk_reward')}"
            )
            coro = data_source.send_email(email_subject, email_body)
            if asyncio.iscoroutine(coro):
                task = asyncio.create_task(coro)
                task.add_done_callback(lambda t: t.exception() if t.exception() else None)
        self.increment_signal_count()

    def _update_peak_profit(self, total: float, activation: float, trail_gap: float) -> None:
        if total > self.highest_session_profit:
            self.highest_session_profit = total
            if self.logger and total > activation:
                self.logger.info(
                    f"📈 Peak profit updated: ${total:.2f} | Locking at ${total - trail_gap:.2f}"
                )

    def _should_lock_profit(self, total: float, activation: float, trail_gap: float) -> bool:
        if self.highest_session_profit < activation:
            return False
        return total <= (self.highest_session_profit - trail_gap)

    async def _liquidate_contracts(self, api: DerivAPI, contracts: List[dict]) -> None:
        await asyncio.gather(
            *[
                api.sell({"sell": contract['contract_id'], "price": 0})
                for contract in contracts
            ]
        )

    def _notify_profit_lock(self, data_source, total: float) -> None:
        if hasattr(data_source, 'send_email'):
            coro = data_source.send_email(
                "Profit Secured",
                f"Closed Basket at ${total:.2f}"
            )
            if asyncio.iscoroutine(coro):
                task = asyncio.create_task(coro)
                task.add_done_callback(lambda t: t.exception() if t.exception() else None)
        self.highest_session_profit = 0.0

    def _structure_precheck(self, df: pd.DataFrame, swing_window: int) -> Optional[Dict[str, Any]]:
        if df is None or df.empty or len(df) < swing_window * 2 + 3:
            return {
                'trend': 'range',
                'is_range': True,
                'last_high': None,
                'last_low': None,
                'swings': [],
            }
        return None

    def _collect_swings(
        self,
        df: pd.DataFrame,
        swing_window: int,
    ) -> Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]], List[Tuple[Any, str, float]]]:
        swings: deque = deque(maxlen=20)
        highs: List[Tuple[Any, float]] = []
        lows: List[Tuple[Any, float]] = []
        for i in range(swing_window, len(df) - swing_window):
            window = df.iloc[i - swing_window:i + swing_window + 1]
            current = df.iloc[i]
            ts = df.index[i] if df.index.size > i else i
            if current['high'] >= window['high'].max():
                highs.append((ts, current['high']))
                swings.append((ts, 'high', current['high']))
            if current['low'] <= window['low'].min():
                lows.append((ts, current['low']))
                swings.append((ts, 'low', current['low']))
        return highs[-3:], lows[-3:], list(swings)

    def _classify_trend(
        self,
        highs: List[Tuple[Any, float]],
        lows: List[Tuple[Any, float]],
    ) -> str:
        if len(highs) < 2 or len(lows) < 2:
            return 'range'
        higher_highs = highs[-1][1] > highs[-2][1]
        higher_lows = lows[-1][1] > lows[-2][1]
        lower_highs = highs[-1][1] < highs[-2][1]
        lower_lows = lows[-1][1] < lows[-2][1]
        if higher_highs and higher_lows:
            return 'bullish'
        if lower_highs and lower_lows:
            return 'bearish'
        return 'range'

    def _apply_score_adjustment(
        self,
        condition: bool,
        score: float,
        reasoning: List[str],
        positive_delta: float,
        positive_message: str,
        negative_delta: float = 0.0,
        negative_message: Optional[str] = None,
    ) -> float:
        if condition:
            score += positive_delta
            if positive_message:
                reasoning.append(positive_message)
        else:
            score += negative_delta
            if negative_message:
                reasoning.append(negative_message)
        return score

    # --- Main Loop ---
    async def analyze_market(self, symbol: str, data_source, trades_this_session: int = 0) -> Optional[dict]:
        data = await self.fetch_all_timeframe_data(symbol, data_source)
        if not data:
            return None

        h4_trend = self.get_h4_trend(data[self.trend_tf_h4])
        h1_trend = self.get_h1_trend(data[self.trend_tf_h1])
        fibs = self.calculate_fibonacci_levels(data[self.trend_tf_h1])
        
        zones = self._collect_zones_from_timeframes(data)
        
        entry_df = data[self.entry_timeframe]
        if entry_df is None:
            return None
        
        current_price = entry_df['close'].iloc[-1]
        filtered_zones = self.filter_zones(zones, current_price)
        self._export_zones_to_csv(filtered_zones)

        structure = self.detect_market_structure(entry_df)
        signal = self.check_entry(entry_df, filtered_zones, fibs, structure, trades_this_session, h4_trend, h1_trend)

        if signal:
            return await self._process_signal(signal, entry_df, structure, h4_trend, h1_trend, data_source)
        return None

    def _collect_zones_from_timeframes(self, data: Dict[str, pd.DataFrame]) -> List[dict]:
        """Collect order blocks and FVG zones from all configured timeframes."""
        zones = []
        for tf in self.zone_timeframes:
            df = data.get(tf)
            if df is None:
                if self.logger:
                    self.logger.warning(f"No data available for timeframe {tf}; skipping zone scan.")
                continue

            order_blocks = self.detect_order_blocks(df, timeframe=tf)
            fvgs = self.detect_fair_value_gaps(df, timeframe=tf)
            zones.extend(order_blocks)
            zones.extend(fvgs)

            if self.logger:
                self.logger.debug(
                    "Timeframe %s produced %d order blocks and %d FVG zones",
                    tf,
                    len(order_blocks),
                    len(fvgs),
                )

        if self.logger:
            self.logger.debug("Total zones collected before filtering: %d", len(zones))
        return zones

    async def _process_signal(
        self,
        signal: dict,
        entry_df: pd.DataFrame,
        structure: dict,
        h4_trend: str,
        h1_trend: str,
        data_source,
    ) -> Optional[dict]:
        """Enrich signal with quality assessment and execute if score is sufficient."""
        signal_id = uuid.uuid4().hex
        signal['id'] = signal_id
        
        quality = self.assess_signal_quality(signal, entry_df, structure, h4_trend, h1_trend)
        self._enrich_signal_with_quality(signal, quality)
        self.save_signal_features(signal_id, quality['features'])

        if self.logger:
            self.logger.debug(
                "Signal %s assessment -> score=%.3f tier=%s rr=%.3f zone_strength=%.3f",
                signal_id,
                quality['score'],
                quality['tier'],
                signal.get('risk_reward', 0.0),
                signal.get('zone_strength', 0.0),
            )

        if quality['score'] >= 0.7:
            return await self._execute_quality_signal(signal, data_source)
        
        self._log_rejected_signal(quality['score'])
        return None

    def _enrich_signal_with_quality(self, signal: dict, quality: Dict[str, Any]) -> None:
        """Add quality metrics to signal dictionary."""
        signal['quality_reasoning'] = quality['reasoning']
        signal['quality_score'] = quality['score']
        signal['quality_tier'] = quality['tier']
        signal['features'] = quality['features']

    async def _execute_quality_signal(self, signal: dict, data_source) -> dict:
        """Execute a signal that passed quality threshold."""
        if self.logger:
            self.logger.info(f"✅ SIGNAL: {signal['signal']} (Score: {signal['quality_score']})")
        
        execution = await self._execute_trade(signal, data_source)
        if execution:
            signal['execution'] = execution
        return signal

    def _log_rejected_signal(self, score: float) -> None:
        """Log signal rejection due to low quality score."""
        if self.logger:
            self.logger.info(f"⚠️ Signal rejected - score below threshold ({score})")

    # --- Utilities ---
    def filter_zones(self, zones, price):
        ranked: List[dict] = []
        for zone in zones:
            distance = abs(zone['price'] - price) / max(price, 1e-6)
            if distance >= 0.02:
                continue
            zone_copy = dict(zone)
            zone_copy['distance'] = distance
            ranked.append(zone_copy)

        ranked.sort(key=lambda z: (z['distance'], -z.get('strength', 0)))

        merged: List[dict] = []
        for zone in ranked:
            if not merged:
                merged.append(zone)
                continue
            last = merged[-1]
            if (
                zone['type'] == last['type']
                and abs(zone['price'] - last['price']) <= self.zone_merge_distance * price
                and zone.get('timeframe') == last.get('timeframe')
            ):
                stronger = zone if zone.get('strength', 0) >= last.get('strength', 0) else last
                merged[-1] = stronger
            else:
                merged.append(zone)
        if self.logger:
            self.logger.debug(
                "Filtered %d raw zones down to %d merged candidates around price %.5f",
                len(zones),
                len(merged),
                price,
            )
        return merged

    def _export_zones_to_csv(self, zones):
        try:
            path = EXPORTS_DIR / 'adapted_zones.csv'
            with open(path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['type', 'low', 'high', 'timeframe', 'subtype', 'strength'])
                for z in zones[:MAX_ZONE_EXPORTS]:
                    subtype = 'OB' if 'OB' in z['type'] else 'FVG'
                    market_side = 'supply' if 'Bearish' in z['type'] else 'demand'
                    writer.writerow([
                        market_side,
                        z['low'],
                        z['high'],
                        z.get('timeframe', '?'),
                        subtype,
                        z.get('strength', 0),
                    ])
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Zone export failed: {exc}")

    def detect_market_structure(self, df: pd.DataFrame, swing_window: int = 5) -> Dict[str, Any]:
        fallback = self._structure_precheck(df, swing_window)
        if fallback is not None:
            if self.logger:
                self.logger.debug("Market structure fallback triggered; insufficient data for swings.")
            return fallback

        highs, lows, swings = self._collect_swings(df, swing_window)
        trend = self._classify_trend(highs, lows)

        if self.logger:
            self.logger.debug(
                "Market structure -> trend: %s | highs: %s | lows: %s | swings=%d",
                trend,
                highs[-1] if highs else None,
                lows[-1] if lows else None,
                len(swings),
            )

        return {
            'trend': trend,
            'is_range': trend == 'range',
            'last_high': highs[-1] if highs else None,
            'last_low': lows[-1] if lows else None,
            'swings': swings,
        }

    def extract_features(self, signal: dict, df: pd.DataFrame, structure: dict) -> Dict[str, float]:
        closes = df['close'].tail(50)
        returns = closes.pct_change().dropna()
        volatility = returns.std() if not returns.empty else 0.0
        distance_to_zone = abs(signal['price'] - signal.get('zone_price', signal['price'])) / max(signal['price'], 1e-6)

        structure_bias = structure.get('trend', 'range')
        structure_alignment = 0
        if structure_bias == 'bullish' and signal['signal'] == 'LONG':
            structure_alignment = 1
        elif structure_bias == 'bearish' and signal['signal'] == 'SHORT':
            structure_alignment = 1
        elif structure_bias != 'range':
            structure_alignment = -1

        features = {
            'trend_alignment': signal.get('trend_alignment', 0),
            'risk_reward': signal.get('risk_reward', 0),
            'zone_strength': signal.get('zone_strength', 0),
            'zone_distance': distance_to_zone,
            'structure_alignment': structure_alignment,
            'volatility': float(volatility),
        }
        return features

    def assess_signal_quality(self, signal: dict, df: pd.DataFrame, structure: dict, h4: str, h1: str) -> Dict[str, Any]:
        features = self.extract_features(signal, df, structure)
        score = 0.5
        reasoning: List[str] = []

        score = self._apply_score_adjustment(
            features['trend_alignment'] == 1,
            score,
            reasoning,
            positive_delta=0.15,
            positive_message='Trend alignment confirmed',
            negative_delta=-0.1,
            negative_message='Trend alignment weak',
        )

        score = self._apply_score_adjustment(
            features['structure_alignment'] == 1,
            score,
            reasoning,
            positive_delta=0.1,
            positive_message='Market structure supports trade',
            negative_delta=-0.15,
            negative_message='Market structure opposes trade',
        )

        score = self._apply_score_adjustment(
            features['risk_reward'] >= self.min_risk_reward,
            score,
            reasoning,
            positive_delta=0.1,
            positive_message='Risk:Reward meets threshold',
            negative_delta=-0.1,
            negative_message='Risk:Reward below preferred level',
        )

        score = self._apply_score_adjustment(
            features['zone_strength'] >= 1.0,
            score,
            reasoning,
            positive_delta=0.05,
            positive_message='Zone strength confirmed',
            negative_delta=0.0,
            negative_message='Zone strength marginal',
        )

        score = self._apply_score_adjustment(
            features['zone_distance'] <= self.zone_buffer * 2,
            score,
            reasoning,
            positive_delta=0.05,
            positive_message='Price interacting with zone',
            negative_delta=-0.05,
            negative_message='Entry distant from zone core',
        )

        score = self._apply_score_adjustment(
            0.0003 <= features['volatility'] <= 0.003,
            score,
            reasoning,
            positive_delta=0.05,
            positive_message='Volatility within ideal band',
            negative_delta=0.0,
            negative_message='Volatility outside ideal band',
        )

        score = self._apply_score_adjustment(
            h4 == h1 == signal['signal'].lower(),
            score,
            reasoning,
            positive_delta=0.05,
            positive_message='Higher timeframe confluence strong',
        )

        score = max(0.0, min(1.0, score))

        if score >= 0.85:
            tier = 'A+'
        elif score >= 0.75:
            tier = 'A'
        elif score >= 0.65:
            tier = 'B'
        else:
            tier = 'C'

        return {
            'score': round(score, 3),
            'tier': tier,
            'reasoning': reasoning,
            'features': features,
        }

    def get_signal_count(self):
        return self._signal_counter

    def increment_signal_count(self):
        self._signal_counter += 1
        return self._signal_counter

    def save_signal_features(self, signal_id: str, features: Dict[str, Any]):
        try:
            path = EXPORTS_DIR / 'signal_features.jsonl'
            record = {
                'id': signal_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'features': features,
            }
            with open(path, 'a') as handle:
                handle.write(json.dumps(record) + '\n')
        except Exception as exc:
            if self.logger:
                self.logger.error(f"Failed to persist signal features: {exc}")
# === End ===