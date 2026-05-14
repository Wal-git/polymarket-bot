"""`polybot doctor` core checks — see main.py for the CLI binding.

Each numbered check returns OK/WARN/FAIL via the console; the caller decides
whether to also run the live order test (step 8) which moves real funds.
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

import httpx
from dotenv import load_dotenv
from rich.console import Console

console = Console()


def _step(num: int, label: str):
    console.print(f"\n[bold cyan][{num}][/] {label}")


def _ok(msg: str):
    console.print(f"    [green]OK[/]   {msg}")


def _warn(msg: str):
    console.print(f"    [yellow]WARN[/] {msg}")


def _fail(msg: str):
    console.print(f"    [red]FAIL[/] {msg}")


def _run() -> tuple[int, int]:
    """Run checks 1–7 and return (failures, warnings)."""
    load_dotenv(Path(".env"))
    failures = 0
    warnings = 0

    # 1. SDK version
    _step(1, "SDK version")
    try:
        from importlib.metadata import version as _v
        installed = _v("py-clob-client-v2")
        latest = None
        try:
            r = httpx.get("https://pypi.org/pypi/py-clob-client-v2/json", timeout=5)
            latest = r.json()["info"]["version"]
        except Exception:
            pass
        if latest and installed != latest:
            _warn(f"installed {installed}, latest on PyPI is {latest} — upgrade via `pip install -U py-clob-client-v2`")
            warnings += 1
        else:
            _ok(f"py-clob-client-v2 {installed} (latest)")
    except Exception as e:
        _fail(f"SDK probe failed: {e}")
        failures += 1

    # 2. Env
    _step(2, "Environment")
    need = ["POLYGON_PRIVATE_KEY", "CLOB_API_KEY", "CLOB_API_SECRET", "CLOB_API_PASSPHRASE"]
    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        _fail(f"missing env vars: {', '.join(missing)}")
        return failures + 1, warnings
    _ok("private key + CLOB creds present")
    if os.environ.get("BUILDER_CODE"):
        _ok("BUILDER_CODE set (rewards attribution active)")
    else:
        _warn("BUILDER_CODE not set — builder rewards disabled")
        warnings += 1

    # Ensure src on path for in-tree imports
    if "src" not in sys.path:
        sys.path.insert(0, "src")

    from polybot.client.clob import CLOBClient, DEPOSIT_WALLET
    from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

    EXPECTED_EOA = "0x7d128C4e9199130fCb0e375476d544a107ab33c2"

    # 3. EOA
    _step(3, "EOA")
    c = CLOBClient(); c.connect()
    addr = c.client.get_address()
    if addr.lower() != EXPECTED_EOA.lower():
        _fail(f"got {addr}, expected {EXPECTED_EOA}")
        failures += 1
    else:
        _ok(addr)

    # 4. Auth
    _step(4, "CLOB auth")
    try:
        c.client.assert_level_1_auth()
        c.client.assert_level_2_auth()
        _ok("L1 + L2 pass")
    except Exception as e:
        _fail(f"auth failed: {e}")
        failures += 1

    # 5. Balance match (CLOB vs on-chain)
    _step(5, "Balance: CLOB-visible vs on-chain")
    try:
        clob_bal = c.get_balance()
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider("https://1rpc.io/matic", request_kwargs={"timeout": 20}))
        PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        ERC20 = [{"inputs":[{"name":"a","type":"address"}],"name":"balanceOf","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"}]
        tok = w3.eth.contract(address=w3.to_checksum_address(PUSD), abi=ERC20)
        onchain = Decimal(tok.functions.balanceOf(w3.to_checksum_address(DEPOSIT_WALLET)).call()) / Decimal("1e6")
        delta = abs(onchain - clob_bal)
        if delta > Decimal("0.01"):
            _warn(f"CLOB={clob_bal} vs on-chain={onchain} (Δ={delta}). Run sync_balance_allowance()")
            warnings += 1
        else:
            _ok(f"both report {clob_bal} pUSD on {DEPOSIT_WALLET}")
    except Exception as e:
        _fail(f"balance check error: {e}")
        failures += 1

    # 6. Proxy allowances
    _step(6, "Proxy allowances on the V2 exchanges")
    try:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider("https://1rpc.io/matic", request_kwargs={"timeout": 20}))
        PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        CTF  = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        V2   = "0xE111180000d2663C0091e4f400237545B87B996B"
        V2NR = "0xe2222d279d744050d28e00520010520000310F59"
        ERC20 = [{"inputs":[{"name":"o","type":"address"},{"name":"s","type":"address"}],"name":"allowance","outputs":[{"type":"uint256"}],"stateMutability":"view","type":"function"}]
        CTF_ABI = [{"inputs":[{"name":"o","type":"address"},{"name":"op","type":"address"}],"name":"isApprovedForAll","outputs":[{"type":"bool"}],"stateMutability":"view","type":"function"}]
        pusd = w3.eth.contract(address=w3.to_checksum_address(PUSD), abi=ERC20)
        ctf  = w3.eth.contract(address=w3.to_checksum_address(CTF),  abi=CTF_ABI)
        for label, addr in [("V2 exch", V2), ("V2 negrisk", V2NR)]:
            allowance = pusd.functions.allowance(w3.to_checksum_address(DEPOSIT_WALLET), w3.to_checksum_address(addr)).call()
            if allowance == 0:
                _fail(f"pUSD allowance to {label} is 0"); failures += 1
            else:
                _ok(f"pUSD → {label}: ∞")
            approved = ctf.functions.isApprovedForAll(w3.to_checksum_address(DEPOSIT_WALLET), w3.to_checksum_address(addr)).call()
            if not approved:
                _fail(f"CTF setApprovalForAll to {label} is False"); failures += 1
            else:
                _ok(f"CTF → {label}: approved")
    except Exception as e:
        _fail(f"allowance check error: {e}")
        failures += 1

    # 7. SDK vs hand-rolled signing parity
    _step(7, "Local signing parity (SDK ↔ hand-rolled)")
    try:
        from polybot.client.v2_order import build_v2_poly_1271_order
        from py_clob_client_v2.order_utils.exchange_order_builder_v2 import ExchangeOrderBuilderV2
        from py_clob_client_v2.order_utils.model.order_data_v2 import OrderDataV2
        from py_clob_client_v2.signer import Signer
        pk = os.environ["POLYGON_PRIVATE_KEY"]
        signer = Signer(pk, 137)
        builder = ExchangeOrderBuilderV2(
            "0xE111180000d2663C0091e4f400237545B87B996B", 137, signer,
            generate_salt=lambda: 7,
        )
        sdk_signed = builder.build_signed_order(OrderDataV2(
            maker=DEPOSIT_WALLET, signer=DEPOSIT_WALLET,
            tokenId="78433024518676680431174478322854148606578065650008220678402966840627347604025",
            makerAmount="50000", takerAmount="5000000", side=0,
            signatureType=SignatureTypeV2.POLY_1271, timestamp="1778728800000",
            metadata="0x" + "0" * 64, builder="0x" + "0" * 64, expiration="0",
        ))
        ours = build_v2_poly_1271_order(
            private_key=pk, deposit_wallet=DEPOSIT_WALLET,
            token_id="78433024518676680431174478322854148606578065650008220678402966840627347604025",
            maker_amount=50000, taker_amount=5000000, side=0,
            timestamp_ms=1778728800000, salt=7,
        )
        if sdk_signed.signature.lower() != ours["signature"].lower():
            _fail("SDK and hand-rolled sigs disagree (regression!)"); failures += 1
        else:
            _ok("signatures identical")
    except Exception as e:
        _fail(f"signing parity error: {e}")
        failures += 1

    return failures, warnings
