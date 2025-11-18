"""
Session manager entry point for the V6 Volatility 25 (1s) bot.

This module supervises the trading session by launching the monitor loop,
tracking realised profit/loss, enforcing daily targets, and ensuring the bot
shuts down gracefully when objectives or risk limits are reached.
"""
# ruff: noqa: E402

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional, Tuple

from dotenv import load_dotenv


load_dotenv()


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Enhanced Deriv trading bot.")
    parser.add_argument(
        "--account-mode",
        choices=["demo", "real"],
        help="Select which Deriv credentials to use (demo or real). Defaults to DERIV_ACCOUNT_MODE env or 'demo'.",
    )
    parser.add_argument("--deriv-app-id", help="Override the Deriv app ID.")
    parser.add_argument("--deriv-token", help="Override the Deriv API token.")
    return parser.parse_args()


def _resolve_deriv_credentials(args: argparse.Namespace) -> Tuple[str, str, str]:
    mode = (args.account_mode or os.environ.get("DERIV_ACCOUNT_MODE") or "demo").strip().lower()
    if mode not in {"demo", "real"}:
        mode = "demo"

    def select_value(explicit: Optional[str], real_key: str, demo_key: str) -> Optional[str]:
        if explicit:
            return explicit
        if mode == "real":
            return os.environ.get(real_key) or os.environ.get(demo_key)
        return os.environ.get(demo_key) or os.environ.get(real_key)

    app_id = select_value(
        args.deriv_app_id or os.environ.get("DERIV_APP_ID"),
        "DERIV_APP_ID_REAL",
        "DERIV_APP_ID_DEMO",
    )
    token = select_value(
        args.deriv_token or os.environ.get("DERIV_API_TOKEN"),
        "DERIV_API_TOKEN_REAL",
        "DERIV_API_TOKEN_DEMO",
    )

    if not app_id or not token:
        raise RuntimeError("Deriv app ID and API token are required to run the bot.")

    return mode, app_id, token


CLI_ARGS = _parse_cli_args()
ACCOUNT_MODE, SELECTED_APP_ID, SELECTED_TOKEN = _resolve_deriv_credentials(CLI_ARGS)

os.environ["DERIV_ACCOUNT_MODE"] = ACCOUNT_MODE
os.environ["DERIV_APP_ID"] = SELECTED_APP_ID
os.environ["DERIV_API_TOKEN"] = SELECTED_TOKEN

from config.settings import DERIV_API_TOKEN, DERIV_APP_ID  # noqa: E402  # pylint: disable=wrong-import-position
from deriv_api import DerivAPI
from scripts.monitor import monitor, request_shutdown

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

        logger.info("Initializing Deriv session manager (%s account).", ACCOUNT_MODE)
        self.app_id = int(DERIV_APP_ID)
        self.api_token = DERIV_API_TOKEN
        self.account_mode = ACCOUNT_MODE
        self.login_id: Optional[str] = None
        self.landing_company: Optional[str] = None
        self._authorization_logged = False
        self.start_balance: Optional[float] = None

    async def _get_balance(self) -> Optional[float]:
        api = DerivAPI(app_id=self.app_id)
        try:
            logger.info("Authorizing with Deriv to fetch account balance.")
            auth = await api.authorize(self.api_token)
            self._record_authorization(auth)
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
            auth = await api.authorize(self.api_token)
            self._record_authorization(auth)
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

    def _record_authorization(self, auth_response: Optional[dict]) -> None:
        if self._authorization_logged or not auth_response:
            return
        authorize = auth_response.get("authorize") if isinstance(auth_response, dict) else None
        if not authorize:
            return
        self.login_id = authorize.get("loginid")
        self.landing_company = authorize.get("landing_company_name")
        account_type = "demo" if authorize.get("is_virtual") else "real"
        if self.login_id:
            logger.info(
                "Authorized Deriv account %s (%s, landing_company=%s)",
                self.login_id,
                account_type,
                self.landing_company or "unknown",
            )
            self._authorization_logged = True

    async def supervise(self) -> None:
        logger.info("Starting supervision loop. Fetching starting balance...")
        self.start_balance = await self._get_balance()
        if self.start_balance is None:
            raise RuntimeError("Unable to determine starting balance; aborting session.")

        logger.info(f"Session start balance: ${self.start_balance:.2f}")

        monitor_task = asyncio.create_task(monitor())

        try:
            await monitor_task
        finally:
            await self._close_all_positions()
            await monitor_task


async def main() -> None:
    manager = SessionManager()
    await manager.supervise()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        request_shutdown()
        logger.info("Session interrupted by user.")

