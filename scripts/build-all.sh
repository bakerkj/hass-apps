#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "${repo_root}"

failed=()
passed=()

for addon in */build.yaml; do
  addon=$(dirname "${addon}")
  image=$(awk '/amd64:/ {print $2}' "${addon}/build.yaml")
  echo "=== Building ${addon} (base: ${image}) ==="
  if docker build --build-arg "BUILD_FROM=${image}" -t "hass-${addon}" "${addon}"; then
    passed+=("${addon}")
  else
    failed+=("${addon}")
  fi
  echo
done

echo "=== Results ==="
for a in "${passed[@]}"; do echo "  PASS: ${a}"; done
for a in "${failed[@]+"${failed[@]}"}"; do echo "  FAIL: ${a}"; done

if [[ ${#failed[@]} -gt 0 ]]; then
  echo "${#failed[@]} addon(s) failed to build."
  exit 1
fi
echo "All ${#passed[@]} addons built successfully."
