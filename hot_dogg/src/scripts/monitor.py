import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from dotenv import load_dotenv

from config import settings
from data_source import DerivDataSource
from email_service import EmailService
from enhanced_strategy import EnhancedZoneStrategy
from signal_tracker import SignalTracker

load_dotenv()

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("monitor")

# --- STRATEGY CONFIG ---
MARKET = settings.MARKET_SYMBOL
MARKET_NAME = settings.MARKET_NAME
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "60"))

signal_tracker = SignalTracker()


class SignalDeduplicator:
    def __init__(self, min_price_change_percent: float = 0.5, cooldown_minutes: int = 15):
        self.min_price_change_percent = min_price_change_percent
        self.cooldown_minutes = cooldown_minutes
        self.last_signal: Optional[dict] = None

    def should_send(self, signal_type: str, entry_price: float, pattern_type: str = "") -> bool:
        now = datetime.now()
        if self.last_signal is None:
            self.last_signal = {
                "type": signal_type,
                "price": entry_price,
                "time": now,
                "pattern": pattern_type,
            }
            logger.info(f"✅ First signal allowed: {signal_type} @ {entry_price:.2f}")
            return True

        elapsed_minutes = (now - self.last_signal["time"]).total_seconds() / 60
        if elapsed_minutes < self.cooldown_minutes:
            logger.info(
                f"❌ Signal rejected: {elapsed_minutes:.1f} min since last < {self.cooldown_minutes} min minimum"
            )
            return False

        price_change_pct = abs(entry_price - self.last_signal["price"]) / max(self.last_signal["price"], 1e-6) * 100
        if price_change_pct < self.min_price_change_percent:
            logger.info(
                f"❌ Signal rejected: price change {price_change_pct:.2f}% < {self.min_price_change_percent}%"
            )
            return False

        self.last_signal = {
            "type": signal_type,
            "price": entry_price,
            "time": now,
            "pattern": pattern_type,
        }
        logger.info(f"✅ Signal allowed: {signal_type} @ {entry_price:.2f}")
        return True


signal_filter = SignalDeduplicator()
# --- EMAIL SERVICE FUNCTIONS ---
async def test_email_service():
    try:
        logger.info("📧 Testing email service configuration...")
        email_service = EmailService(
            smtp_server=os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            smtp_port=int(os.getenv('SMTP_PORT', '587')),
            username=os.getenv('EMAIL_USERNAME'),
            password=os.getenv('EMAIL_PASSWORD')
        )
        alert_email = os.getenv('ALERT_EMAIL')
        if not alert_email:
            logger.error("❌ ALERT_EMAIL environment variable not set")
            return False
        await email_service.send_email(
            subject="✅ Enhanced Zone Strategy v2.1 Email Test",
            body=f"Enhanced strategy with Signal Tracking and Performance Analytics enabled. Test sent at {datetime.now()}",
            to_email=alert_email
        )
        logger.info("✅ You will be receiving signals every 15 minutes maximum! Trade carefully!")
        return True
    except Exception as e:
        logger.error(f"❌ Email service test failed: {e}")
        return False

async def send_signal_email(signal: dict):
    try:
        signal_type = signal.get('signal', 'UNKNOWN')
        entry_price = signal.get('price', 0)
        pattern_type = signal.get('pattern', '')
        
        # Apply smart filtering with 15-minute intervals
        if not signal_filter.should_send(signal_type, entry_price, pattern_type):
            logger.info("📧 Email notification skipped - 15-minute interval not met")
            return False
            
        email_service = EmailService(
            smtp_server=os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            smtp_port=int(os.getenv('SMTP_PORT', '587')),
            username=os.getenv('EMAIL_USERNAME'),
            password=os.getenv('EMAIL_PASSWORD')
        )
        alert_email = os.getenv('ALERT_EMAIL')
        if not alert_email:
            logger.warning("❌ ALERT_EMAIL not set - cannot send signal email")
            return False

        if 'range_reversal' in pattern_type:
            pattern_emoji = "🔄"
            market_type = "RANGE"
        elif 'continuation' in pattern_type:
            pattern_emoji = "📈" if signal_type == 'LONG' else "📉"
            market_type = "TREND"
        elif 'pullback' in pattern_type:
            pattern_emoji = "↩️"
            market_type = "PULLBACK"
        else:
            pattern_emoji = "📊"
            market_type = "SIGNAL"

        reasoning_list = signal.get('quality_reasoning', [])
        if isinstance(reasoning_list, list):
            reasoning_text = "\n- ".join([""] + reasoning_list) if reasoning_list else "N/A"
        else:
            reasoning_text = str(reasoning_list)

        subject = f"{pattern_emoji} {signal_type} {market_type}: {entry_price:.2f} {MARKET_NAME} [{signal.get('quality_tier', 'UNKNOWN')}]"
        body = f"""
🚀 ENHANCED ZONE STRATEGY v6.0 SIGNAL

Signal Type: {signal_type}
Market Mode: {market_type}
Quality: {signal.get('quality_tier', 'UNKNOWN')} ({signal.get('quality_score', 0):.2f})
Entry Price: {entry_price:.2f}

Stop Loss: {signal.get('stop_loss', 0):.2f}
Take Profit: {signal.get('take_profit', 0):.2f}

📊 TRADE DETAILS:
Zone Type: {signal.get('zone_type', 'unknown')}
Zone Price: {signal.get('zone_price', 0):.2f}
Zone Strength: {signal.get('zone_strength', 0):.2f}
Risk:Reward: {signal.get('risk_reward', 0):.2f}:1
Pattern: {pattern_type}

🔍 Quality Assessment:{reasoning_text if reasoning_text != 'N/A' else ' N/A'}
⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏱️ Next Signal: 15+ minutes

📊 Performance Stats: run ~/trading_bot/get_signal_report.sh for detailed metrics

Good luck! 🍀
"""
        await email_service.send_email(subject, body, alert_email)
        logger.info(f"✅ {market_type} signal email sent: {signal_type} @ {entry_price:.2f} (Next in 15+ min)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send signal email: {e}")
        return False
# --- ENHANCED SESSION LOGGER WITH 15-MINUTE INTERVAL TRACKING ---
class SessionLogger:
    def __init__(self):
        self.start_time = datetime.now()
        self.total_analyses = 0
        self.signals_generated = 0
        self.emails_sent = 0
        self.cycles = 0
        self.trend_signals = 0
        self.range_signals = 0
        self.pullback_signals = 0
        self.continuation_signals = 0
        self.long_signals = 0
        self.short_signals = 0
        self.unique_signals = set()  # Track unique signal signatures
        self.signal_timestamps = {}  # Track when each signal type was last sent
        self.signals_blocked = 0     # Track blocked signals due to interval
        
    def is_unique_signal(self, signal):
        """Check if this is a truly unique trading opportunity with 15-minute intervals"""
        signal_signature = f"{signal.get('signal')}__{signal.get('pattern')}__{signal.get('price', 0):.0f}__{signal.get('zone_price', 0):.0f}"
        current_time = datetime.now()
        
        # Check if exact signal exists
        if signal_signature in self.unique_signals:
            return False
            
        # Check time-based interval (15 minutes)
        signal_type_key = f"{signal.get('signal')}__{signal.get('pattern')}"
        if signal_type_key in self.signal_timestamps:
            last_signal_time = self.signal_timestamps[signal_type_key]
            time_diff = (current_time - last_signal_time).total_seconds() / 60  # minutes
            
            if time_diff < 15:  # 15 minute interval
                self.signals_blocked += 1
                logger.info(f"⏰ Signal blocked: {signal_type_key} sent {time_diff:.1f} minutes ago (need 15+ minutes)")
                return False
        
        # Signal is unique and respects time interval
        self.unique_signals.add(signal_signature)
        self.signal_timestamps[signal_type_key] = current_time
        return True

    def log_analysis(self, zones_info=None):
        """Log analysis cycle completion"""
        self.total_analyses += 1
        if zones_info:
            logger.info(f"📊 Analysis #{self.total_analyses}: {zones_info}")
    def log_signal(self, signal, email_sent=False):
        """Log signal generation and classification"""
        signal_type = signal.get('signal', 'UNKNOWN')
        pattern = signal.get('pattern', '')
        entry_price = signal.get('price', 0)
        rr = signal.get('risk_reward', 0)
        
        self.signals_generated += 1
        if email_sent:
            self.emails_sent += 1
        
        # Track signal types
        if signal_type == 'LONG':
            self.long_signals += 1
        elif signal_type == 'SHORT':
            self.short_signals += 1
            
        # Track pattern types
        if 'range_reversal' in pattern:
            self.range_signals += 1
            logger.info(f"🔄 RANGE {signal_type}: {entry_price:.2f} | RR: {rr:.1f} | Pattern: {pattern}")
        elif 'continuation' in pattern:
            self.continuation_signals += 1
            self.trend_signals += 1
            logger.info(f"📈 CONTINUATION {signal_type}: {entry_price:.2f} | RR: {rr:.1f} | Pattern: {pattern}")
        elif 'pullback' in pattern:
            self.pullback_signals += 1
            self.trend_signals += 1
            logger.info(f"↩️ PULLBACK {signal_type}: {entry_price:.2f} | RR: {rr:.1f} | Pattern: {pattern}")
        else:
            self.trend_signals += 1
            logger.info(f"📊 SIGNAL {signal_type}: {entry_price:.2f} | RR: {rr:.1f} | Pattern: {pattern}")

    def log_cycle(self, cycle_number):
        """Log cycle completion"""
        self.cycles += 1
        logger.info(f"Analysis cycle #{cycle_number} complete.")
    def log_session_summary(self):
        """Log comprehensive session summary"""
        runtime = datetime.now() - self.start_time
        runtime_str = str(runtime).split('.')[0]  # Remove microseconds
        
        logger.info("=" * 70)
        logger.info("📊 ENHANCED STRATEGY SESSION SUMMARY (15-MIN INTERVALS)")
        logger.info("-" * 70)
        logger.info(f"⏱️  Runtime: {runtime_str}")
        logger.info(f"🔍 Total analyses: {self.total_analyses}")
        logger.info(f"📡 Signals generated: {self.signals_generated}")
        logger.info(f"📧 Emails sent: {self.emails_sent}")
        logger.info(f"⏰ Signals blocked (interval): {self.signals_blocked}")
        logger.info(f"🔄 Cycles completed: {self.cycles}")
        logger.info(f"🎯 Unique opportunities: {len(self.unique_signals)}")
        logger.info("-" * 30)
        logger.info("📊 Signal Breakdown:")
        logger.info(f"   📈 Trend signals: {self.trend_signals}")
        logger.info(f"     ↗️ Continuation: {self.continuation_signals}")
        logger.info(f"     ↩️ Pullback: {self.pullback_signals}")
        logger.info(f"   🔄 Range signals: {self.range_signals}")
        logger.info(f"   🟢 LONG signals: {self.long_signals}")
        logger.info(f"   🔴 SHORT signals: {self.short_signals}")
        if self.total_analyses > 0:
            signal_rate = (self.signals_generated / self.total_analyses) * 100
            email_rate = (self.emails_sent / self.signals_generated * 100) if self.signals_generated > 0 else 0
            block_rate = (self.signals_blocked / self.signals_generated * 100) if self.signals_generated > 0 else 0
            logger.info(f"   📊 Signal rate: {signal_rate:.1f}%")
            logger.info(f"   📧 Email rate: {email_rate:.1f}%")
            logger.info(f"   ⏰ Block rate: {block_rate:.1f}%")
        
        # Add signal performance tracking metrics
        try:
            signal_report_path = settings.PROJECT_ROOT / "get_signal_report.sh"
            if signal_report_path.exists():
                logger.info("-" * 30)
                logger.info("📊 For detailed signal performance metrics, run:")
                logger.info(f"   {signal_report_path}")
        except Exception:
            pass
            
        logger.info("=" * 70)

session_logger = SessionLogger()
shutdown_event = asyncio.Event()

# --- SHUTDOWN HANDLING ---
def handle_shutdown(signum, frame):
    logger.info("🛑 Shutdown signal received. Exiting after current cycle.")
    shutdown_event.set()


def request_shutdown():
    shutdown_event.set()
# --- ENHANCED MAIN MONITOR LOOP WITH 15-MINUTE INTERVALS AND SIGNAL TRACKING ---

# --- ML MODEL INTEGRATION ---
def predict_ml_success(signal):
    """Predict the success probability of a signal using ML model"""
    try:
        import joblib
        import numpy as np
        
        model_path = Path.home() / "trading_bot" / "ml_trade_model.pkl"
        
        if not model_path.exists():
            logger.warning("ML model not found, allowing trade (fallback).")
            return 0.70  # Default success probability
            
        # Load the model
        model = joblib.load(model_path)
        
        # Extract features from signal
        features = [
            float(signal.get('trend_alignment', 0)),  # Trend alignment
            float(signal.get('is_range_market', 0)),  # Is range market
            float(signal.get('zone_strength', 0.5)),  # Zone strength
            float(signal.get('zone_proximity', 0.5)),  # Zone proximity
            float(signal.get('is_continuation', 0)),  # Is continuation pattern
            float(signal.get('is_pullback', 0)),      # Is pullback pattern
            float(signal.get('is_reversal', 0)),      # Is reversal pattern
            float(signal.get('risk_reward', 1.0))     # Risk-reward ratio
        ]
        
        # Make prediction
        features_array = np.array([features])
        success_probability = model.predict_proba(features_array)[0, 1]
        
        logger.info(f"ML predicted success probability: {success_probability:.2f} (threshold: 0.45)")
        return success_probability
        
    except Exception as e:
        logger.warning(f"ML prediction error: {e}, allowing trade")
        return 0.70  # Default success probability on error


async def handle_signal(
    signal_dict: dict,
    strategy: EnhancedZoneStrategy,
    data_source: DerivDataSource,
) -> bool:
    logger.info(
        f"[{datetime.now()}] ✅ SIGNAL FOUND: {signal_dict['signal']} @ "
        f"{signal_dict['price']:.2f} (Score: {signal_dict.get('quality_score')})"
    )

    is_unique = session_logger.is_unique_signal(signal_dict)
    if not is_unique:
        session_logger.log_signal(signal_dict, email_sent=False)
        logger.info("🔄 Signal blocked - within 15-minute interval or duplicate")
        return False

    if signal_dict.get("ready_for_execution"):
        execution = await strategy.execute_signal(signal_dict, data_source)
        if execution:
            signal_dict['execution'] = execution
    else:
        logger.info("✅ Signal logged but not marked ready for execution; skipping trade.")
        session_logger.log_signal(signal_dict, email_sent=False)
        return False

    signal_tracker.record_signal(signal_dict)
    email_sent = await send_signal_email(signal_dict)
    session_logger.log_signal(signal_dict, email_sent=email_sent)
    if not email_sent:
        logger.info("📧 Signal generated but email filtered by smart deduplicator")
    return True


def _resolve_current_price(data_source, signal_dict: Optional[dict]) -> float:
    try:
        if hasattr(data_source, "current_price"):
            return float(getattr(data_source, "current_price"))
        if signal_dict:
            return float(signal_dict.get("price", 0))
    except Exception as exc:
        logger.error(f"Error getting current price: {exc}")
    return 0.0


def _log_tracker_updates(updates: Dict[str, int]) -> None:
    if not updates:
        return
    wins = updates.get("wins", 0)
    losses = updates.get("losses", 0)
    if wins or losses:
        logger.info(f"📊 Signal tracker updated: {wins} wins, {losses} losses")


def update_signal_outcomes(
    cycle_count: int,
    data_source,
    signal_dict: Optional[dict],
) -> None:
    if cycle_count % 10 != 0:
        return
    current_price = _resolve_current_price(data_source, signal_dict)
    if current_price <= 0:
        return
    try:
        updates = signal_tracker.update_outcomes(current_price)
        _log_tracker_updates(updates)
    except Exception as exc:
        logger.error(f"Error updating signal outcomes: {exc}")


async def run_analysis_cycle(
    cycle_count: int,
    strategy: EnhancedZoneStrategy,
    data_source: DerivDataSource,
) -> None:
    logger.info(
        f"🔄 Running analysis cycle #{cycle_count} at "
        f"{datetime.now().strftime('%H:%M:%S')}"
    )

    trades_this_session = strategy.get_signal_count()
    signal_dict = await strategy.analyze_market(MARKET, data_source, trades_this_session)
    session_logger.log_analysis()

    if signal_dict:
        await handle_signal(signal_dict, strategy, data_source)
    else:
        logger.info("No valid signal this cycle.")

    update_signal_outcomes(cycle_count, data_source, signal_dict)
    session_logger.log_cycle(cycle_count)


async def wait_for_next_cycle() -> None:
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=ANALYSIS_INTERVAL)
    except asyncio.TimeoutError:
        pass


async def send_startup_email() -> None:
    try:
        alert_email = os.getenv("ALERT_EMAIL")
        if not alert_email:
            logger.warning("ALERT_EMAIL not set—startup email skipped.")
            return

        email_service = EmailService(
            smtp_server=os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
        )
        await email_service.send_email(
            subject="📡 Bot Monitor Started",
            body=f"Monitor booted at {datetime.now():%Y-%m-%d %H:%M:%S}.",
            to_email=alert_email,
        )
        logger.info("📧 Startup email sent.")
    except Exception as exc:
        logger.error(f"Startup email failed: {exc}")


async def monitor():
    shutdown_event.clear()
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    await send_startup_email()
    
    logger.info("=" * 70)
    logger.info("🚀 Enhanced Zone Strategy v2.1 Monitor Starting")
    logger.info(f"📈 Market: {MARKET_NAME}")
    logger.info(f"🔄 Analysis Interval: {ANALYSIS_INTERVAL}s")
    logger.info("⏰ Email Intervals: 15 minutes minimum")
    logger.info("✨ Features: Smart Pattern Detection, 15-Min Signal Spacing")
    logger.info("🎯 Improvements: Signal Performance Tracking, Enhanced Analytics")
    logger.info("=" * 70)
    
    data_source = DerivDataSource(
        symbol=MARKET,
        email_service=EmailService(
            smtp_server=os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
        ),
        alert_email=os.getenv("ALERT_EMAIL"),
    )
    strategy = EnhancedZoneStrategy(logger)
    cycle_count = 0
    while not shutdown_event.is_set():
        try:
            cycle_count += 1
            await run_analysis_cycle(cycle_count, strategy, data_source)
        except Exception as e:
            logger.error(f"❌ Error in monitor loop: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

        await wait_for_next_cycle()
    
    session_logger.log_session_summary()
    logger.info("🏁 Enhanced Monitor with signal tracking stopped gracefully.")
if __name__ == "__main__":
    try:
        asyncio.run(test_email_service())
        asyncio.run(monitor())
    except KeyboardInterrupt:
        print("\n🛑 Program interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
