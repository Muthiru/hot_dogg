# Volatility 25 (1s) Sniper Bot — V6.0

Professional, production-ready implementation of the V25(1s) trend-following trading bot.  
The bot aligns higher-timeframe trends, hunts demand/supply zones (OB/FVG), layers into
positions through Deriv’s API, and protects profit with an elastic trailing stop. A
session manager enforces business rules so the system can run unattended on Google Cloud.

---

## Features

- **Strict Trend Filter** – EMA slope detection prevents trading in sideways markets.
- **Multi-Timeframe Confluence** – H4 + H1 + entry timeframe must agree before the bot acts.
- **Zone Intelligence** – Automatically detects order blocks and fair value gaps, exports
  top zones for your MT5 overlay.
- **5-Layer Execution** – DCA-style order placement with server-side hard stops to guard
  against connection loss.
- **Elastic Trailing Stop** – Locks gains once profit reaches \$5 and trails by \$2 across
  the basket.
- **Session Governance** – `run_bot.py` stops the session after +\$50 target, -\$30 max
  loss, or 12 hours (configurable), sending email alerts for each outcome. Optional
  performance-based bonus tiers can extend the profit cap when recent results meet
  quality thresholds.
- **Signal Analytics** – Scorecards, feature exports, and a tracker that records
  performance for ongoing optimisation.

---

## Project Structure

```
src/
├── config/settings.py        # Centralised configuration + environment lookups
├── data_source.py            # Async Deriv WebSocket client with caching
├── email_service.py          # Minimal SMTP helper for alerts
├── enhanced_strategy.py      # Core trading logic (trend, zones, execution, trailing)
├── run_bot.py                # Session manager & business rules
├── signal_tracker.py         # Records signals and outcomes for analytics
└── scripts/monitor.py        # Main loop: fetch data, evaluate strategy, send notifications
```

Supporting artefacts:

- `.env.example` – Reference for required environment variables.
- `requirements.txt` – Runtime dependencies.

---

## Getting Started

### 1. Clone & Setup Python

```bash
git clone https://github.com/<you>/hot_dogg.git
cd hot_dogg

python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

> **Python version**: 3.9+ recommended (tested with 3.10/3.11).

### 2. Configure Environment

Copy the sample env file and fill in your secrets:

```bash
cp env.example .env
```

Required values:

- `
