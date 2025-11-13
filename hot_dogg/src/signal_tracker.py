"""
Signal tracking utilities for the V6 strategy.

The tracker records every signal emitted by the strategy, maintains a rolling
view of open opportunities, and updates win/loss counts based on subsequent
price movements.  Data is persisted as JSON lines so that long-running cloud
processes can be inspected or restored easily.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import DATA_DIR


@dataclass
class TrackedSignal:
    signal_id: str
    direction: str
    entry_price: float
    stop_loss: float
    take_profit: float
    timestamp: str
    metadata: Dict[str, float] = field(default_factory=dict)
    status: str = "open"

    def to_dict(self) -> Dict[str, object]:
        return {
            "id": self.signal_id,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "timestamp": self.timestamp,
            "status": self.status,
            "metadata": self.metadata,
        }


class SignalTracker:
    def __init__(self, storage_path: Optional[Path] = None) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.storage_path = storage_path or (DATA_DIR / "signals.jsonl")
        self.signals: List[TrackedSignal] = []
        self.wins = 0
        self.losses = 0
        self._load_existing()

    def _load_existing(self) -> None:
        if not self.storage_path.exists():
            return
        try:
            with open(self.storage_path, "r") as handle:
                for line in handle:
                    payload = json.loads(line.strip())
                    tracked = TrackedSignal(
                        signal_id=payload["id"],
                        direction=payload["direction"],
                        entry_price=payload["entry_price"],
                        stop_loss=payload["stop_loss"],
                        take_profit=payload["take_profit"],
                        timestamp=payload["timestamp"],
                        metadata=payload.get("metadata", {}),
                        status=payload.get("status", "open"),
                    )
                    if tracked.status == "win":
                        self.wins += 1
                    elif tracked.status == "loss":
                        self.losses += 1
                    self.signals.append(tracked)
        except Exception:
            # If loading fails we start fresh; corrupted lines will be ignored.
            self.signals = []
            self.wins = 0
            self.losses = 0

    def _persist(self, tracked: TrackedSignal) -> None:
        with open(self.storage_path, "a") as handle:
            handle.write(json.dumps(tracked.to_dict()) + "\n")

    def record_signal(self, signal: Dict[str, object]) -> None:
        tracked = TrackedSignal(
            signal_id=str(signal.get("id")),
            direction=str(signal.get("signal")),
            entry_price=float(signal.get("price", 0)),
            stop_loss=float(signal.get("stop_loss", 0)),
            take_profit=float(signal.get("take_profit", 0)),
            timestamp=datetime.now(timezone.utc).isoformat(),
            metadata={
                "quality": float(signal.get("quality_score", 0)),
                "tier": signal.get("quality_tier", "NA"),
                "zone": signal.get("zone_type", ""),
            },
        )
        self.signals.append(tracked)
        self._persist(tracked)

    def _evaluate_outcome(self, tracked: TrackedSignal, current_price: float) -> Optional[str]:
        if tracked.direction == "LONG":
            if current_price >= tracked.take_profit:
                return "win"
            if current_price <= tracked.stop_loss:
                return "loss"
        elif tracked.direction == "SHORT":
            if current_price <= tracked.take_profit:
                return "win"
            if current_price >= tracked.stop_loss:
                return "loss"
        return None

    def _apply_outcome(self, tracked: TrackedSignal, outcome: str, updates: Dict[str, int]) -> None:
        tracked.status = outcome
        if outcome == "win":
            self.wins += 1
            updates["wins"] += 1
        elif outcome == "loss":
            self.losses += 1
            updates["losses"] += 1

    def update_outcomes(self, current_price: float) -> Dict[str, int]:
        updates = {"wins": 0, "losses": 0}
        for tracked in self.signals:
            if tracked.status != "open":
                continue
            outcome = self._evaluate_outcome(tracked, current_price)
            if outcome:
                self._apply_outcome(tracked, outcome, updates)

        if updates["wins"] or updates["losses"]:
            # Rewrite file to capture updated statuses.
            with open(self.storage_path, "w") as handle:
                for tracked in self.signals:
                    handle.write(json.dumps(tracked.to_dict()) + "\n")
        return updates

    def summary(self) -> Dict[str, int]:
        return {
            "wins": self.wins,
            "losses": self.losses,
            "open": sum(1 for s in self.signals if s.status == "open"),
            "total": len(self.signals),
        }

    def recent_performance(self, count: int) -> Dict[str, float]:
        """
        Return aggregate performance metrics for the most recent closed signals.

        Args:
            count: Number of closed signals to include in the slice.

        Returns:
            Dict with keys:
                `count`: number of closed signals considered,
                `win_rate`: ratio of wins within that slice (0.0 if none),
                `average_score`: average quality score, ignoring missing values.
        """
        closed: List[TrackedSignal] = []
        for tracked in reversed(self.signals):
            if tracked.status in {"win", "loss"}:
                closed.append(tracked)
                if len(closed) >= count:
                    break

        if not closed:
            return {"count": 0, "win_rate": 0.0, "average_score": 0.0}

        closed.reverse()
        wins = sum(1 for tracked in closed if tracked.status == "win")
        quality_values = [
            float(tracked.metadata.get("quality", 0.0))
            for tracked in closed
            if tracked.metadata.get("quality") is not None
        ]
        avg_quality = sum(quality_values) / len(quality_values) if quality_values else 0.0

        return {
            "count": len(closed),
            "win_rate": wins / len(closed),
            "average_score": avg_quality,
        }


