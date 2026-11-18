#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$repo_root/deployments/compose.yaml"

docker build -t evalfrog-sandbox-python:dev -f "$repo_root/deployments/sandbox/Dockerfile" "$repo_root/deployments/sandbox"
docker compose -f "$compose_file" up -d --build --wait
docker compose -f "$compose_file" run --build --rm --no-deps evalfrog-cli doctor --profile local --config-dir /app/configs

echo 'EvalFrog M0 stack is ready.'
