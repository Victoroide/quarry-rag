#!/bin/sh
set -eu

targets=${FUZZ_INVENTORY-$(go run ./scripts/fuzz -package="${FUZZ_PACKAGE:-}")}
if [ -z "$targets" ]; then
  printf '%s\n' 'No fuzz targets assigned' >&2
  exit 1
fi
budget=${FUZZTIME-30s}
failed=0
tab=$(printf '\t')

while IFS="$tab" read -r package_path target; do
  if ! go test -run='^$' -fuzz="^${target}\$" -fuzztime="$budget" "$package_path" < /dev/null; then
    printf 'Fuzz target failed: %s %s\n' "$package_path" "$target" >&2
    failed=1
  fi
done <<EOF
$targets
EOF

exit "$failed"
