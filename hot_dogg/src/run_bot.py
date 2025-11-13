"""
Session manager entry point for the V6 Volatility 25 (1s) bot.

This module supervises the trading session by launching the monitor loop,
tracking realised profit/loss, enforcing daily targets, and ensuring the bot
shuts down gracefully when objectives or risk limits are reached.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional, Tuple

from config.settings import (
    DAILY_TARGET,
    DERIV_APP_ID,
    DERIV_API_TOKEN,
    EMAIL_MAX_LOSS_SUBJECT,
    EMAIL_TARGET_HIT_SUBJECT,
    BONUS_TARGET,
    BONUS_MIN_RECENT_TRADES,
    BONUS_MIN_AVG_SCORE,
    BONUS_MIN_WIN_RATE,
    MAX_DAILY_LOSS,
    MAX_SESSION_DURATION,
)
from deriv_api import DerivAPI
from email_service import EmailService
from scripts.monitor import monitor, request_shutdown, signal_tracker

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("session")


class SessionManager:
    def __init__(self) -> None:
        if not DERIV_APP_ID or not DERIV_API_TOKEN:
            raise RuntimeError("Deriv credentials must be provided via environment or config settings.")

        logger.info("Initializing Deriv session manager.")
        self.app_id = int(DERIV_APP_ID)
        self.api_token = DERIV_API_TOKEN
        self.start_time: datetime = datetime.now(timezone.utc)
        self.start_balance: Optional[float] = None
        self.bonus_active = False
        self.bonus_enabled = BONUS_TARGET > max(DAILY_TARGET, 0.0)
        self.email_service = EmailService(
            smtp_server=os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            smtp_port=int(os.getenv("SMTP_PORT", "587")),
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
        )
        self.alert_email = os.getenv("ALERT_EMAIL")

    async def _get_balance(self) -> Optional[float]:
        api = DerivAPI(app_id=self.app_id)
        try:
            logger.info("Authorizing with Deriv to fetch account balance.")
            await api.authorize(self.api_token)
            balance_response = await api.balance()
            balance = float(balance_response["balance"]["balance"])
            logger.info("Current balance retrieved successfully: $%.2f", balance)
            return balance
        except Exception as exc:
            logger.error(f"Failed to fetch balance: {exc}")
            return None
        finally:
            try:
                await api.disconnect()
            except Exception:
                pass

    async def _close_all_positions(self) -> None:
        api = DerivAPI(app_id=self.app_id)
        try:
            await api.authorize(self.api_token)
            portfolio = await api.portfolio()
            contracts = portfolio.get("portfolio", {}).get("contracts", [])
            if not contracts:
                return
            await asyncio.gather(
                *[api.sell({"sell": contract["contract_id"], "price": 0}) for contract in contracts]
            )
            logger.info(f"Closed {len(contracts)} open contracts at shutdown.")
        except Exception as exc:
            logger.error(f"Failed to close open positions: {exc}")
        finally:
            try:
                await api.disconnect()
            except Exception:
                pass

    async def _send_alert(self, subject: str, body: str) -> None:
        if not self.alert_email:
            logger.warning("Alert email not configured; skipping notification.")
            return
        try:
            await self.email_service.send_email(subject, body, self.alert_email)
        except Exception as exc:
            logger.error(f"Failed to send session alert: {exc}")

    async def supervise(self) -> None:
        logger.info("Starting supervision loop. Fetching starting balance...")
        self.start_balance = await self._get_balance()
        if self.start_balance is None:
            raise RuntimeError("Unable to determine starting balance; aborting session.")

        logger.info(f"Session start balance: ${self.start_balance:.2f}")

        monitor_task = asyncio.create_task(monitor())

        try:
            while not monitor_task.done():
                await asyncio.sleep(60)
                should_stop, reason = await self._evaluate_limits()
                if should_stop:
                    logger.info(f"Session limit reached: {reason}. Initiating shutdown.")
                    request_shutdown()
                    break
        finally:
            await self._close_all_positions()
            await monitor_task

    async def _evaluate_limits(self) -> Tuple[bool, str]:
        now = datetime.now(timezone.utc)
        elapsed = now - self.start_time
        if elapsed >= MAX_SESSION_DURATION:
            await self._send_alert(
                subject="Session Timeout",
                body=f"Trading session reached the maximum duration ({MAX_SESSION_DURATION}).",
            )
            return True, "time limit reached"

        balance = await self._get_balance()
        if balance is None:
            return False, ""

        profit = balance - self.start_balance
        if profit >= DAILY_TARGET:
            bonus_action = await self._handle_bonus_tier(profit)
            if bonus_action == "continue":
                return False, ""
            if bonus_action == "bonus_hit":
                return True, "bonus target achieved"
            await self._send_alert(
                subject=EMAIL_TARGET_HIT_SUBJECT,
                body=f"Daily profit target reached: ${profit:.2f}. Bot shutting down.",
            )
            return True, "daily target achieved"

        if profit <= MAX_DAILY_LOSS:
            await self._send_alert(
                subject=EMAIL_MAX_LOSS_SUBJECT,
                body=f"Daily loss limit reached: ${profit:.2f}. Bot shutting down to protect capital.",
            )
            return True, "daily loss limit hit"

        return False, ""

    async def _handle_bonus_tier(self, profit: float) -> Optional[str]:
        """
        Evaluate and manage the bonus profit tier.

        Returns:
            Optional[str]:
                "continue"   -> keep running towards bonus target,
                "bonus_hit"  -> bonus target achieved, caller should shut down,
                None         -> fall back to standard daily target handling.
        """
        if not self.bonus_enabled:
            return None

        if not signal_tracker:
            return None

        if self.bonus_active:
            if BONUS_TARGET <= DAILY_TARGET or BONUS_TARGET <= 0:
                return None
            if profit >= BONUS_TARGET:
                await self._send_alert(
                    subject="Bonus Target Hit",
                    body=f"Extended profit target reached: ${profit:.2f}. Bot shutting down.",
                )
                return "bonus_hit"
            return "continue"  # Continue session towards bonus target.

        performance = signal_tracker.recent_performance(BONUS_MIN_RECENT_TRADES)
        required_trades = max(1, BONUS_MIN_RECENT_TRADES)
        if performance["count"] < required_trades:
            return None
        if performance["average_score"] < BONUS_MIN_AVG_SCORE:
            return None
        if performance["win_rate"] < BONUS_MIN_WIN_RATE:
            return None

        self.bonus_active = True
        logger.info(
            "Bonus tier activated: extending profit cap to $%.2f "
            "(avg score %.2f, win rate %.2f across %d trades).",
            BONUS_TARGET,
            performance["average_score"],
            performance["win_rate"],
            performance["count"],
        )
        await self._send_alert(
            subject="Bonus Tier Activated",
            body=(
                "Recent performance met quality thresholds. Extending session "
                f"target to ${BONUS_TARGET:.2f}. Current profit: ${profit:.2f}."
            ),
        )
        return "continue"


async def main() -> None:
    manager = SessionManager()
    await manager.supervise()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        request_shutdown()
        logger.info("Session interrupted by user.")

