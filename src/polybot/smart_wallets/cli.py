"""CLI entrypoint: python -m polybot.smart_wallets.cli [run|status]"""
from __future__ import annotations

import json
import sys

import structlog

logger = structlog.get_logger()


def _cmd_run(args: list[str]) -> None:
    import polybot.smart_wallets.pipeline as pipeline

    dry_run = "--dry-run" in args
    limit = None
    for a in args:
        if a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])

    result = pipeline.run(dry_run=dry_run, candidate_limit=limit)
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
    store.close()


def main(argv: list[str] | None = None) -> None:
    argv = argv or sys.argv[1:]
    cmd = argv[0] if argv else "run"
    rest = argv[1:]

    if cmd == "run":
        _cmd_run(rest)
    elif cmd == "status":
        _cmd_status(rest)
    else:
        print(f"Unknown command: {cmd}. Use 'run' or 'status'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
