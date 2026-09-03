#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# PODMAN_USER_ID defaults to $(id -u); override for podman machine: PODMAN_USER_ID=$(podman machine ssh id -u)
export PODMAN_USER_ID="${PODMAN_USER_ID:-$(id -u)}"
export CONTAINER_SOCKET_HOST_PATH="${CONTAINER_SOCKET_HOST_PATH:-/run/user/${PODMAN_USER_ID}/podman/podman.sock}"
exec podman compose "$@"
