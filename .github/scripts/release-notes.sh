#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo 'usage: release-notes.sh <tag>' >&2
  exit 1
fi

version=${1#v}
awk -v heading="[$version]" '
  /^## \[/ {
    if (selected) exit
    selected = ($2 == heading)
    next
  }
  selected {
    body = body $0 "\n"
    if ($0 ~ /[^[:space:]]/) nonempty = 1
  }
  END {
    if (!nonempty) {
      print "missing or empty CHANGELOG section " heading > "/dev/stderr"
      exit 1
    }
    printf "%s", body
  }
' CHANGELOG.md
