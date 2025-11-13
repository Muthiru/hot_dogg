import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import websockets

from config.settings import DERIV_ACCOUNT_MODE, DERIV_APP_ID, DERIV_API_TOKEN, MARKET_SYMBOL, TIMEFRAME_SECONDS

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)
class DerivDataSource:
    def __init__(
        self,
        app_id=None,
        api_token=None,
        symbol: Optional[str] = None,
        email_service=None,
        alert_email: Optional[str] = None,
    ):
        self.app_id = app_id or DERIV_APP_ID
        self.api_token = api_token or DERIV_API_TOKEN
        self.symbol = symbol or MARKET_SYMBOL
        if not self.app_id:
            raise ValueError("DERIV_APP_ID is required for data access")
        if not self.api_token:
            raise ValueError("DERIV_API_TOKEN is required for data access")
        self.ws_url = f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"
        self.logger = logging.getLogger(__name__)
        # Initialize cache
        self.cached_data = {}
        self.cache_ttl = timedelta(minutes=5)  # 5-minute TTL for cache
        self.current_price = 0  # Initialize current price
        self.account_mode = os.getenv("DERIV_ACCOUNT_MODE", DERIV_ACCOUNT_MODE)
        self.login_id: Optional[str] = None
        self.landing_company: Optional[str] = None
        self.is_virtual: Optional[bool] = None
        self._authorization_logged = False
        self.email_service = email_service
        self.alert_email = alert_email

    async def _send_ws_request(self, request_data, retries: int = 3, retry_delay: float = 0.5):
        """Send a request to the Deriv API using WebSockets"""
        attempt = 0
        while attempt < retries:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    if self.api_token:
                        auth_request = {"authorize": self.api_token}
                        await websocket.send(json.dumps(auth_request))
                        auth_response = await websocket.recv()
                        auth_data = json.loads(auth_response)

                        if "error" in auth_data:
                            self.logger.error(f"Authorization failed: {auth_data['error']['message']}")
                            return None
                        self._record_authorization(auth_data)

                    await websocket.send(json.dumps(request_data))
                    response = await websocket.recv()
                    return json.loads(response)
            except Exception as e:
                attempt += 1
                self.logger.error(f"WebSocket request error (attempt {attempt}/{retries}): {str(e)}")
                if attempt >= retries:
                    return None
                await asyncio.sleep(retry_delay * attempt)
    def _record_authorization(self, auth_data: dict) -> None:
        if self._authorization_logged:
            return
        authorize = auth_data.get("authorize")
        if not authorize:
            return
        self.login_id = authorize.get("loginid")
        self.landing_company = authorize.get("landing_company_name")
        self.is_virtual = bool(authorize.get("is_virtual"))
        account_type = "demo" if self.is_virtual else "real"
        self.logger.info(
            "Authorized data feed for Deriv account %s (%s, landing_company=%s)",
            self.login_id or "unknown",
            account_type,
            self.landing_company or "unknown",
        )
        self._authorization_logged = True

    async def send_email(self, subject: str, body: str) -> None:
        if not self.email_service or not self.alert_email:
            return
        try:
            await self.email_service.send_email(subject, body, self.alert_email)
        except Exception as exc:
            self.logger.error(f"Failed to send data source email: {exc}")
    async def get_candles(self, symbol: str = None, interval: int = None, count: int = 100) -> Optional[pd.DataFrame]:
        """Get historical candles from Deriv API"""
        try:
            symbol = symbol or self.symbol
            if interval is None:
                raise ValueError("Interval must be provided for candle retrieval")

            timeframe_name = self._seconds_to_timeframe(interval)

            if timeframe_name in self.cached_data:
                cache_entry = self.cached_data[timeframe_name]
                if datetime.now() < cache_entry['expires_at']:
                    self.logger.debug(f"Using cached data for {symbol} {timeframe_name}")
                    return cache_entry['data']

            self.logger.info(f"Fetching {count} candles for {symbol} @ {interval}s interval")

            end_epoch = int(datetime.now().timestamp())
            start_epoch = end_epoch - (interval * count)
            request_data = {
                "ticks_history": symbol,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "start": start_epoch,
                "style": "candles",
                "granularity": interval,
            }

            data = await self._send_ws_request(request_data)

            if not data or "error" in data:
                if data and "error" in data:
                    self.logger.error(f"API error: {data['error']['message']}")
                return None

            if not data.get("candles"):
                self.logger.warning(f"No candles returned for {symbol}")
                return None

            candles = []
            for candle in data["candles"]:
                candles.append({
                    "time": pd.to_datetime(candle["epoch"], unit="s"),
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": int(candle.get("volume", 0)),
                })

            df = pd.DataFrame(candles)

            if not df.empty:
                self.current_price = df.iloc[-1]['close']

            if timeframe_name:
                ttl = self._ttl_for_timeframe(timeframe_name)
                self.cached_data[timeframe_name] = {
                    'data': df,
                    'timestamp': datetime.now(),
                    'expires_at': datetime.now() + ttl,
                }

            return df

        except Exception as e:
            self.logger.error(f"Error fetching candles: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    def clear_cache(self):
        """Clear all cached data"""
        self.cached_data = {}
        self.logger.info("🔄 Cache cleared")
        
    def force_fresh_data(self, timeframe: str = None):
        """Force fresh data fetch by clearing specific timeframe cache"""
        if timeframe:
            if timeframe in self.cached_data:
                del self.cached_data[timeframe]
                self.logger.info(f"Cleared cache for {timeframe}")
        else:
            self.clear_cache()
            
    def get_cache_status(self):
        """Get current cache status"""
        status = {}
        for timeframe, cache_entry in self.cached_data.items():
            status[timeframe] = {
                'cached_candles': len(cache_entry['data']) if cache_entry['data'] is not None else 0,
                'expires_at': cache_entry['expires_at'].strftime('%H:%M:%S'),
                'is_expired': datetime.now() > cache_entry['expires_at']
            }
        return status
    async def get_historical_data(self, symbol: str, timeframe: str, periods: int = 100) -> pd.DataFrame:
        """Get historical data - wrapper for get_candles to match enhanced strategy interface"""
        try:
            
            # Convert timeframe to seconds (intervals)
            timeframe_seconds = self._timeframe_to_seconds(timeframe)
            
            # Use existing get_candles method
            df = await self.get_candles(
                symbol=symbol,
                interval=timeframe_seconds,
                count=periods
            )
            
            if df is not None and not df.empty:
                # Rename columns to match what enhanced strategy expects
                df = df.rename(columns={'time': 'timestamp'})
                # Set timestamp as index if it's not already
                if 'timestamp' in df.columns:
                    df.set_index('timestamp', inplace=True)
                
                self.logger.info(f"Historical data retrieved: {len(df)} candles for {symbol} {timeframe}")
                return df
            else:
                self.logger.warning(f"No historical data retrieved for {symbol} {timeframe}")
                return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error in get_historical_data: {e}")
            return pd.DataFrame()

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert timeframe string to seconds for Deriv API"""
        return TIMEFRAME_SECONDS.get(timeframe, 60)

    def _seconds_to_timeframe(self, seconds: int) -> Optional[str]:
        for tf, value in TIMEFRAME_SECONDS.items():
            if value == seconds:
                return tf
        return None

    def _ttl_for_timeframe(self, timeframe: str) -> timedelta:
        if timeframe in ('4H', '1H'):
            return timedelta(minutes=15)
        if timeframe in ('30M', '15M'):
            return timedelta(minutes=5)
        return timedelta(minutes=2)
            
    async def get_ohlc_data(self, symbol: str, granularity: int, count: int = 100) -> pd.DataFrame:
        """Alternative method name that some parts of the code might expect"""
        try:
            interval_seconds = granularity
            
            return await self.get_candles(symbol, interval_seconds, count)
            
        except Exception as e:
            self.logger.error(f"Error in get_ohlc_data: {e}")
            return pd.DataFrame()
            
    async def get_latest_price(self, symbol):
        """Get the latest price for a given symbol."""
        if hasattr(self, 'current_price') and self.current_price:
            return self.current_price
            
        # If current price is not available, try to fetch latest candle
        try:
            df = await self.get_candles(symbol, 60, 1)  # Get just 1 candle with 1-minute interval
            if df is not None and not df.empty:
                self.current_price = df.iloc[-1]['close']
                return self.current_price
        except Exception as e:
            self.logger.error(f"Error fetching latest price: {e}")
        
        # Fallback: return 0 if all else fails
        return 0
