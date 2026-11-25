#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo 'usage: release-inputs.sh <base> <merge-commit>' >&2
  exit 1
fi

paths=$(mktemp)
trap 'rm -f "$paths"' EXIT
# A rename must count both the old and new paths.
if ! git diff --no-ext-diff --name-only --no-renames -z "$1" "$2" -- > "$paths"; then
  echo true
  exit 0
fi

# A root package could embed files anywhere in its subtree.
if compgen -G '*.go' > /dev/null; then
  echo true
  exit 0
fi

while IFS= read -r -d '' path; do
  if [[ ${path%/*} == docs && $path == *.md ]] && ! compgen -G 'docs/*.go' > /dev/null; then
    continue
  fi
  echo true
  exit 0
done < "$paths"
echo false
