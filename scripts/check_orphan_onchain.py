"""Check whether the 22 recovered orphan slugs have on-chain CTF tokens
still unredeemed in the proxy wallet, or whether the winnings have already
been claimed as USDC.

Reads recovered=true rows from results.jsonl, looks up each slug's token_id
from the PM2 entry_fill_confirmed logs, and queries:
  - data-api.polymarket.com /positions  →  shares still held + redeemable flag
  - CTF.balanceOf(proxy, token_id)      →  ground-truth ERC-1155 balance
  - USDC.balanceOf(proxy)               →  current wallet USDC
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import httpx
from web3 import Web3

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

PROXY = "0xd31f05af327955214f71f9387c93ed8bd7f5770c"
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RPC = "https://polygon.drpc.org"
PM2_LOG = Path("/root/.pm2/logs/polymarket-bot-error.log")
RESULTS = REPO_ROOT / "data" / "results.jsonl"

CTF_BAL_ABI = [{
    "inputs": [{"name": "owner", "type": "address"}, {"name": "id", "type": "uint256"}],
    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view", "type": "function",
}]
ERC20_BAL_ABI = [{
    "inputs": [{"name": "a", "type": "address"}],
    "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view", "type": "function",
}]

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def load_recovered_slugs() -> list[str]:
    slugs = []
    for line in RESULTS.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("recovered") and "slug" in r:
            slugs.append(r["slug"])
    return slugs


def load_token_ids(slugs: set[str]) -> dict[str, str]:
    """Stream PM2 logs for entry_fill_confirmed events to map slug → token_id.

    PM2 log can be huge (hundreds of MB across restart cycles), so we stream
    line-by-line and skip non-matching lines fast.
    """
    out: dict[str, str] = {}
    if not PM2_LOG.exists():
        return out
    pattern = re.compile(r"entry_fill_confirmed.*?slug=(\S+).*?token_id=(\d+)")
    with PM2_LOG.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            if "entry_fill_confirmed" not in raw:
                continue
            line = ANSI.sub("", raw)
            m = pattern.search(line)
            if m:
                slug, tid = m.group(1), m.group(2)
                if slug in slugs:
                    out[slug] = tid
                    if len(out) == len(slugs):
                        break
    return out


def main() -> int:
    slugs = load_recovered_slugs()
    if not slugs:
        print("No recovered=true rows in results.jsonl — nothing to check.")
        return 0
    print(f"Checking {len(slugs)} recovered slugs against proxy {PROXY}\n")

    token_ids = load_token_ids(set(slugs))
    missing_tid = [s for s in slugs if s not in token_ids]
    if missing_tid:
        print(f"WARN: {len(missing_tid)} slugs have no token_id in PM2 logs — skipping:")
        for s in missing_tid:
            print(f"  {s}")
        print()

    # data-api positions for the proxy
    print("Querying data-api positions...")
    resp = httpx.get(
        f"https://data-api.polymarket.com/positions?user={PROXY}&sizeThreshold=0",
        timeout=15,
    )
    resp.raise_for_status()
    positions = resp.json()
    by_event: dict[str, dict] = {}
    for p in positions:
        es = p.get("eventSlug")
        if es:
            by_event[es] = p

    # On-chain CTF balances
    w3 = Web3(Web3.HTTPProvider(RPC))
    ctf = w3.eth.contract(address=w3.to_checksum_address(CTF), abi=CTF_BAL_ABI)
    usdc = w3.eth.contract(address=w3.to_checksum_address(USDC), abi=ERC20_BAL_ABI)
    proxy_cs = w3.to_checksum_address(PROXY)

    usdc_bal = usdc.functions.balanceOf(proxy_cs).call() / 1e6
    print(f"\nProxy USDC balance: ${usdc_bal:.2f}\n")

    print(f"{'slug':<32}  {'shares':>10}  {'redeemable':>10}  {'on-chain shares':>16}  {'cur$':>6}")
    print("-" * 90)
    still_held_total = 0.0
    redeemable_total = 0.0
    for slug in sorted(slugs):
        pos = by_event.get(slug)
        api_shares = float(pos.get("size", 0)) if pos else 0.0
        redeemable = bool(pos.get("redeemable")) if pos else False
        cur_price = float(pos.get("curPrice", 0)) if pos else 0.0

        tid = token_ids.get(slug)
        onchain_shares = 0.0
        if tid:
            raw = ctf.functions.balanceOf(proxy_cs, int(tid)).call()
            onchain_shares = raw / 1e6  # CTF tokens use 6 decimals on Polymarket

        if onchain_shares > 0:
            still_held_total += onchain_shares * cur_price if cur_price else onchain_shares
        if redeemable:
            redeemable_total += api_shares * cur_price if cur_price else api_shares

        print(f"{slug:<32}  {api_shares:>10.2f}  {str(redeemable):>10}  {onchain_shares:>16.2f}  {cur_price:>6.2f}")

    print(f"\nTotal value still held on-chain: ${still_held_total:.2f}")
    print(f"Of which marked redeemable: ${redeemable_total:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
