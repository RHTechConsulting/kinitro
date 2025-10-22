#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "${REPO_ROOT}"

PY_CMD=${PYTHON_CMD:-python3}
if ! command -v "${PY_CMD}" >/dev/null 2>&1; then
    echo "${PY_CMD} not found in PATH" >&2
    exit 1
fi

CONFIGS=("bob.toml" "eve.toml")
SLEEP_SECONDS=${DEMO_SLEEP_SECONDS:-60}

extract_submission_id() {
    sed -n 's/.*Submission ID: \([0-9]\+\).*/\1/p' | tail -n1
}

run_cycle() {
    local cfg=$1

    if [[ ! -f ${cfg} ]]; then
        echo "Config ${cfg} missing; skipping." >&2
        return
    fi

    echo "[${cfg}] uploading submission" >&2
    local upload_output
    if ! upload_output=$("${PY_CMD}" -m miner --config "${cfg}" upload 2>&1); then
        echo "[${cfg}] upload failed" >&2
        printf '%s\n' "${upload_output}" >&2
        return
    fi

    printf '%s\n' "${upload_output}" >&2
    local submission_id
    submission_id=$(printf '%s' "${upload_output}" | extract_submission_id)

    if [[ -z ${submission_id} ]]; then
        echo "[${cfg}] could not determine submission id" >&2
        return
    fi

    echo "[${cfg}] committing submission ${submission_id}" >&2
    if ! "${PY_CMD}" -m miner --config "${cfg}" commit --submission-id "${submission_id}"; then
        echo "[${cfg}] commit failed for submission ${submission_id}" >&2
        return
    fi

    echo "[${cfg}] submission ${submission_id} committed successfully" >&2
}

while true; do
    for cfg in "${CONFIGS[@]}"; do
        run_cycle "${cfg}"
    done

    echo "Sleeping for ${SLEEP_SECONDS}s before next round..." >&2
    sleep "${SLEEP_SECONDS}"
done
