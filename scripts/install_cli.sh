#!/usr/bin/env bash
# Build and install the polymarket-cli Rust binary.
# Run from the repo root: bash scripts/install_cli.sh
set -euo pipefail

CLI_DIR="$(dirname "$0")/../tools/polymarket-cli"
cd "$CLI_DIR"
echo "Building polymarket-cli (release)..."
cargo build --release
echo "Done: $CLI_DIR/target/release/polymarket-cli"
