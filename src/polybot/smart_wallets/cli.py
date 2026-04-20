"""CLI entrypoint: python -m polybot.smart_wallets.cli [run|status]"""
from __future__ import annotations

import json
import sys

import structlog

logger = structlog.get_logger()


def _cmd_run(args: list[str]) -> None:
    import polybot.smart_wallets.pipeline as pipeline

    dry_run = "--dry-run" in args
    async_enrich = "--async" in args
    limit = None
    for a in args:
        if a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])

    result = pipeline.run(dry_run=dry_run, candidate_limit=limit, async_enrich=async_enrich)
    print(
        f"\nRun #{result['run_id']} complete: "
        f"{result['n_candidates']} candidates → {result['n_selected']} selected"
        + (" [DRY RUN]" if result["dry_run"] else "")
    )
    if result["wallets"]:
        print("\nTop 5 wallets:")
        for w in result["wallets"][:5]:
            print(
                f"  {w['proxy_wallet'][:12]}… "
                f"score={w['score']:.3f} "
                f"pnl=${w['pnl_realized']:,.0f} "
                f"wr={w['win_rate']:.0%} "
                f"vol=${w['volume']:,.0f}"
            )


def _cmd_status(args: list[str]) -> None:
    from polybot.smart_wallets.store import Store

    store = Store()
    runs = store.recent_runs(limit=5)
    if not runs:
        print("No runs recorded yet.")
        store.close()
        return

    print("\nRecent runs:")
    for r in runs:
        print(
            f"  run_id={r['run_id']}  {r['started_at'][:19]}  "
            f"status={r['status']}  "
            f"candidates={r['n_candidates']}  selected={r['n_selected']}"
        )

    latest = runs[0]
    diff = store.wallet_diff(latest["run_id"])
    print(
        f"\nDiff vs prev run: "
        f"+{len(diff['added'])} added, "
        f"-{len(diff['removed'])} removed, "
        f"={len(diff['retained'])} retained"
    )
    rejects = store.reject_histogram(latest["run_id"])
    if rejects:
        print("\nReject reasons:")
        for reason, n in sorted(rejects.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {n}")
    store.close()


def _cmd_backtest(args: list[str]) -> None:
    from polybot.smart_wallets.backtest import evaluate_run

    run_id = None
    forward_days = 7
    for a in args:
        if a.startswith("--run="):
            run_id = int(a.split("=", 1)[1])
        elif a.startswith("--days="):
            forward_days = int(a.split("=", 1)[1])
    if run_id is None:
        print("Usage: backtest --run=<run_id> [--days=7]")
        sys.exit(1)

    result = evaluate_run(run_id=run_id, forward_days=forward_days)
    print(json.dumps(result.to_dict(), indent=2))


def _cmd_trail_backtest(args: list[str]) -> None:
    from polybot.smart_wallets.trail_backtest import run_trail_backtest

    run_id = None
    window_days = 7
    interval = 30
    for a in args:
        if a.startswith("--run="):
            run_id = int(a.split("=", 1)[1])
        elif a.startswith("--days="):
            window_days = int(a.split("=", 1)[1])
        elif a.startswith("--interval="):
            interval = int(a.split("=", 1)[1])
    if run_id is None:
        print("Usage: trail-backtest --run=<run_id> [--days=7] [--interval=30]")
        sys.exit(1)

    result = run_trail_backtest(
        run_id=run_id, lookback_window_days=window_days, sim_interval_seconds=interval
    )
    print(result.summary())
    print(json.dumps(result.to_dict(), indent=2))


def main(argv: list[str] | None = None) -> None:
    argv = argv or sys.argv[1:]
    cmd = argv[0] if argv else "run"
    rest = argv[1:]

    if cmd == "run":
        _cmd_run(rest)
    elif cmd == "status":
        _cmd_status(rest)
    elif cmd == "backtest":
        _cmd_backtest(rest)
    elif cmd == "trail-backtest":
        _cmd_trail_backtest(rest)
    else:
        print(f"Unknown command: {cmd}. Use 'run', 'status', 'backtest', or 'trail-backtest'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
