#!/usr/bin/env bash

set -euo pipefail

# Initialize version counter
version=1.4

# Run continuously
while true; do
    echo "Running miner commit with version $version"

    # Run both miner commits with current version
    python -m miner commit --chain-commitment-version "$version" && \
    # python -m miner commit --wallet-name charlie --hotkey-name hotkey0 --chain-commitment-version "$version"

    # Increment version
    version=$(echo "$version + 0.1" | bc)

    echo "Sleeping for 120 seconds..."
    sleep 120
done
