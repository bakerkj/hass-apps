#!/usr/bin/env bash
set -euo pipefail

docker build -q -t ha-addon-linter \
  https://github.com/frenck/action-addon-linter.git#main:src >/dev/null

rc=0
for f in "$@"; do
  # ``realpath -m`` resolves ``dirname "${f}"`` to an absolute path
  # whether the caller passes a relative arg (pre-commit does) or an
  # absolute one (manual invocation), so the docker mount source is
  # always a clean absolute path.
  d=$(realpath -m -- "$(dirname -- "${f}")")
  docker run --rm \
    -e INPUT_PATH=/data \
    -e INPUT_COMMUNITY=false \
    -v "${d}:/data" \
    ha-addon-linter || rc=1
done
exit "${rc}"
