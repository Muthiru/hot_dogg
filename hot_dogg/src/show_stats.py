"""CLI utility to display signal tracker performance stats."""

from __future__ import annotations

import argparse
from pathlib import Path

from signal_tracker import SignalTracker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show signal tracker statistics.")
    parser.add_argument(
        "--recent",
        type=int,
        default=10,
        help="Number of recent closed trades to include in performance metrics (default: 10).",
    )
    parser.add_argument(
        "--storage",
        type=Path,
        help="Optional path to signals.jsonl; defaults to DATA_DIR/signals.jsonl.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tracker = SignalTracker(storage_path=args.storage)
    summary = tracker.summary()
    recent = tracker.recent_performance(args.recent)

    print("=== Signal Tracker Summary ===")
    print(f"Wins:   {summary['wins']}")
    print(f"Losses: {summary['losses']}")
    print(f"Open:   {summary['open']}")
    print(f"Total:  {summary['total']}")
    print()
    print(f"=== Recent Performance (last {args.recent}) ===")
    print(f"Closed signals considered: {recent['count']}")
    win_rate = recent['win_rate'] * 100 if recent['count'] else 0.0
    print(f"Win rate: {win_rate:.2f}%")
    print(f"Average quality score: {recent['average_score']:.3f}")


if __name__ == "__main__":
    main()

