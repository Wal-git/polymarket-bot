"""Check balances across all known wallets."""
from decimal import Decimal
from web3 import Web3

RPC = "https://polygon.drpc.org"

PUSD     = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
USDC_E   = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC     = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native USDC
MATIC    = None  # native

ERC20_ABI = [
    {"inputs":[{"name":"account","type":"address"}],"name":"balanceOf",
     "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"decimals",
     "outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
]

WALLETS = {
    "EOA (MetaMask/main)":          "0x7d128C4e9199130fCb0e375476d544a107ab33c2",
    "Proxy POLY_1271 (broken)":     "0xd31f05af327955214f71f9387c93ed8bd7f5770c",
    "Old Gnosis Safe":              "0x05586e36b88f12d0a5287eecadba6f6a5f55e439",
    "Old proxy (no allowances)":    "0xefe7b666033a7572e0c07a452aad1777ef582788",
}

TOKENS = {
    "pUSD":   (PUSD,   6),
    "USDC.e": (USDC_E, 6),
    "USDC":   (USDC,   6),
}


def check(w3: Web3):
    print(f"{'Wallet':<32} {'pUSD':>10} {'USDC.e':>10} {'USDC':>8} {'MATIC':>10}")
    print("-" * 75)
    for label, addr in WALLETS.items():
        cs = w3.to_checksum_address(addr)
        balances = {}
        for name, (tok_addr, dec) in TOKENS.items():
            tok = w3.eth.contract(address=w3.to_checksum_address(tok_addr), abi=ERC20_ABI)
            raw = tok.functions.balanceOf(cs).call()
            balances[name] = Decimal(raw) / Decimal(10 ** dec)
        matic_wei = w3.eth.get_balance(cs)
        matic = Decimal(matic_wei) / Decimal(10 ** 18)
        print(f"{label:<32} {str(balances['pUSD']):>10} {str(balances['USDC.e']):>10} "
              f"{str(balances['USDC']):>8} {str(round(matic, 4)):>10}")
    print()


def main():
    w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 15}))
    assert w3.is_connected(), "RPC not connected"
    print(f"Connected to Polygon (block {w3.eth.block_number})\n")
    check(w3)


if __name__ == "__main__":
    main()
