"""
Recover pUSD from the old Gnosis Safe (0x05586e36...) back to the EOA.
Gnosis Safe v1.3.0 — 1-of-1 multisig, EOA is the sole owner.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account
from eth_utils import keccak
import eth_abi

load_dotenv(Path(".env"))

RPC      = "https://polygon.drpc.org"
CHAIN_ID = 137

EOA   = "0x7d128C4e9199130fCb0e375476d544a107ab33c2"
SAFE  = "0x05586e36b88f12d0a5287eecadba6f6a5f55e439"
PUSD  = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# ─── ABIs ────────────────────────────────────────────────────────────────────
SAFE_ABI = [
    {"inputs": [], "name": "nonce",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [
        {"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
        {"name": "data", "type": "bytes"}, {"name": "operation", "type": "uint8"},
        {"name": "safeTxGas", "type": "uint256"}, {"name": "baseGas", "type": "uint256"},
        {"name": "gasPrice", "type": "uint256"}, {"name": "gasToken", "type": "address"},
        {"name": "refundReceiver", "type": "address"}, {"name": "_nonce", "type": "uint256"},
    ], "name": "getTransactionHash",
     "outputs": [{"type": "bytes32"}], "stateMutability": "view", "type": "function"},
    {"inputs": [
        {"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
        {"name": "data", "type": "bytes"}, {"name": "operation", "type": "uint8"},
        {"name": "safeTxGas", "type": "uint256"}, {"name": "baseGas", "type": "uint256"},
        {"name": "gasPrice", "type": "uint256"}, {"name": "gasToken", "type": "address"},
        {"name": "refundReceiver", "type": "address"}, {"name": "signatures", "type": "bytes"},
    ], "name": "execTransaction",
     "outputs": [{"name": "success", "type": "bool"}], "stateMutability": "payable", "type": "function"},
]
ERC20_ABI = [
    {"inputs": [{"name": "account", "type": "address"}], "name": "balanceOf",
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
]

# ─── Setup ───────────────────────────────────────────────────────────────────
w3    = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 15}))
safe  = w3.eth.contract(address=w3.to_checksum_address(SAFE), abi=SAFE_ABI)
token = w3.eth.contract(address=w3.to_checksum_address(PUSD), abi=ERC20_ABI)
key   = os.environ["POLYGON_PRIVATE_KEY"]
acct  = Account.from_key(key)
assert acct.address.lower() == EOA.lower(), "Private key doesn't match EOA"

# ─── Read state ──────────────────────────────────────────────────────────────
balance = token.functions.balanceOf(w3.to_checksum_address(SAFE)).call()
nonce   = safe.functions.nonce().call()
print(f"Safe pUSD balance : {balance / 1e6:.6f}")
print(f"Safe nonce        : {nonce}")

if balance == 0:
    print("Nothing to recover.")
    exit(0)

# ─── Build the inner call: pUSD.transfer(EOA, balance) ───────────────────────
transfer_data = (
    "a9059cbb"  # transfer(address,uint256)
    + eth_abi.encode(
        ["address", "uint256"],
        [w3.to_checksum_address(EOA), balance],
    ).hex()
)

ZERO = "0x0000000000000000000000000000000000000000"
tx_params = dict(
    to=w3.to_checksum_address(PUSD),
    value=0,
    data=bytes.fromhex(transfer_data),
    operation=0,    # CALL
    safeTxGas=0,
    baseGas=0,
    gasPrice=0,
    gasToken=ZERO,
    refundReceiver=ZERO,
)

# ─── Compute Safe transaction hash (EIP-712) ─────────────────────────────────
SAFE_TX_TYPEHASH = keccak(
    b"SafeTx(address to,uint256 value,bytes data,uint8 operation,"
    b"uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,"
    b"address gasToken,address refundReceiver,uint256 nonce)"
)
DOMAIN_TYPEHASH = keccak(b"EIP712Domain(uint256 chainId,address verifyingContract)")

safe_tx_hash = safe.functions.getTransactionHash(
    tx_params["to"], tx_params["value"], tx_params["data"],
    tx_params["operation"], tx_params["safeTxGas"], tx_params["baseGas"],
    tx_params["gasPrice"], tx_params["gasToken"], tx_params["refundReceiver"],
    nonce,
).call()
print(f"Safe tx hash      : 0x{safe_tx_hash.hex()}")

# ─── Sign (ECDSA, v=27/28, no Ethereum message prefix for Safe) ──────────────
# Gnosis Safe expects a raw EIP-712 signature without the "\x19Ethereum Signed Message" prefix
signed = acct.unsafe_sign_hash(safe_tx_hash)
signature = signed.signature.hex()
print(f"Signature         : 0x{signature[:20]}...")

# ─── Dry-run ─────────────────────────────────────────────────────────────────
print("\nDry-running execTransaction…")
try:
    tx_calldata = safe.encode_abi(
        "execTransaction",
        args=[
            tx_params["to"], tx_params["value"], tx_params["data"],
            tx_params["operation"], tx_params["safeTxGas"], tx_params["baseGas"],
            tx_params["gasPrice"], tx_params["gasToken"], tx_params["refundReceiver"],
            bytes.fromhex(signature),
        ],
    )
    result = w3.eth.call({
        "to": w3.to_checksum_address(SAFE),
        "data": tx_calldata,
        "from": acct.address,
    })
    print(f"Dry-run OK — result: {result.hex()}")
except Exception as e:
    print(f"Dry-run FAILED: {e}")
    exit(1)

# ─── Estimate gas and send ───────────────────────────────────────────────────
print("\nEstimating gas…")
gas_price = w3.eth.gas_price
print(f"Gas price: {gas_price / 1e9:.2f} gwei")

tx = safe.functions.execTransaction(
    tx_params["to"], tx_params["value"], tx_params["data"],
    tx_params["operation"], tx_params["safeTxGas"], tx_params["baseGas"],
    tx_params["gasPrice"], tx_params["gasToken"], tx_params["refundReceiver"],
    bytes.fromhex(signature),
).build_transaction({
    "from": acct.address,
    "nonce": w3.eth.get_transaction_count(acct.address),
    "gasPrice": gas_price,
    "chainId": CHAIN_ID,
})
gas_est = w3.eth.estimate_gas(tx)
tx["gas"] = int(gas_est * 1.2)
print(f"Gas estimate: {gas_est}  (sending with {tx['gas']})")

signed_tx = acct.sign_transaction(tx)
print("\nSending transaction…")
txhash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
print(f"TX submitted: 0x{txhash.hex()}")

receipt = w3.eth.wait_for_transaction_receipt(txhash, timeout=120)
print(f"Status: {'SUCCESS' if receipt.status == 1 else 'FAILED'}")
print(f"Block:  {receipt.blockNumber}")
after = token.functions.balanceOf(w3.to_checksum_address(SAFE)).call()
print(f"Safe pUSD balance after: {after / 1e6:.6f}")
eoa_pusd = token.functions.balanceOf(w3.to_checksum_address(EOA)).call()
print(f"EOA pUSD balance after:  {eoa_pusd / 1e6:.6f}")
