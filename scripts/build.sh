#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# Optional first argument selects an explicit webui checkout. Go build options
# and cross-compilation environment retain their usual meaning.
if [ "$#" -gt 0 ]; then
  node "$root/scripts/build-webui.mjs" "$1"
  shift
else
  node "$root/scripts/build-webui.mjs"
fi
cd "$root"
go build -o dubi "$@" ./cmd/dubi
