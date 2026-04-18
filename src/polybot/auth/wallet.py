import os
from pathlib import Path

import structlog
from dotenv import load_dotenv

logger = structlog.get_logger()


def load_env(env_path: Path | None = None):
    path = env_path or Path(".env")
    load_dotenv(path)


def get_private_key() -> str:
    key = os.environ.get("POLYGON_PRIVATE_KEY", "")
    if not key:
        raise RuntimeError("POLYGON_PRIVATE_KEY not set — run 'polybot setup'")
    return key


def get_clob_creds() -> dict:
    api_key = os.environ.get("CLOB_API_KEY", "")
    api_secret = os.environ.get("CLOB_API_SECRET", "")
    passphrase = os.environ.get("CLOB_API_PASSPHRASE", "")
    if not all([api_key, api_secret, passphrase]):
        raise RuntimeError("CLOB credentials not set — run 'polybot setup'")
    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "passphrase": passphrase,
    }


def run_setup_wizard():
    from getpass import getpass

    env_path = Path(".env")
    print("\n=== Polymarket Bot Setup ===\n")

    private_key = getpass("Enter your Polygon wallet private key: ").strip()
    if not private_key:
        print("No key provided. Aborting.")
        return

    print("\nDeriving CLOB API credentials...")
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.constants import POLYGON

        client = ClobClient(
            "https://clob.polymarket.com",
            key=private_key,
            chain_id=POLYGON,
        )
        creds = client.create_or_derive_api_creds()
        api_key = creds.api_key
        api_secret = creds.api_secret
        api_passphrase = creds.api_passphrase
    except Exception as e:
        print(f"\nFailed to derive credentials: {e}")
        print("You can manually set them in .env")
        api_key = input("CLOB_API_KEY (or leave blank): ").strip()
        api_secret = input("CLOB_API_SECRET (or leave blank): ").strip()
        api_passphrase = input("CLOB_API_PASSPHRASE (or leave blank): ").strip()

    env_path.write_text(
        f"POLYGON_PRIVATE_KEY={private_key}\n"
        f"CLOB_API_KEY={api_key}\n"
        f"CLOB_API_SECRET={api_secret}\n"
        f"CLOB_API_PASSPHRASE={api_passphrase}\n"
    )

    print(f"\nCredentials saved to {env_path.resolve()}")
    print("\nNext steps:")
    print("  1. Fund your wallet with USDC on Polygon")
    print("  2. Run: polybot dry-run")
    print("  3. When ready: set dry_run: false in config/default.yaml")

    logger.info("setup_complete")
