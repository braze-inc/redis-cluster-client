#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export PODMAN_USER_ID="${PODMAN_USER_ID:-$(id -u)}"
exec "${CONTAINER_RUNTIME:-podman}" compose "$@"
