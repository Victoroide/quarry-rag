#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo 'usage: third-party-licenses.sh <package> <output-dir>' >&2
  exit 1
fi

export LC_ALL=C
modules=$(mktemp)
trap 'rm -f "$modules" "$modules.sorted"' EXIT HUP INT TERM
go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}@{{.Version}} {{.Dir}}{{end}}{{end}}' "$1" > "$modules"
sort -u "$modules" > "$modules.sorted"
mkdir -p "$2"
output=$(cd "$2" && pwd)

while IFS=' ' read -r module directory; do
  [ -n "$module" ] || continue
  found=false
  for file in "$directory"/*; do
    [ -f "$file" ] || continue
    name=${file##*/}
    lower=$(printf '%s' "$name" | tr '[:upper:]' '[:lower:]')
    case "$lower" in
      license*|licence*|copying*|notice*|patents*)
        mkdir -p "$output/$module"
        cp "$file" "$output/$module/$name"
        found=true
        ;;
    esac
  done
  if [ "$found" = false ]; then
    echo "no license or notice files found for $module" >&2
    exit 1
  fi
done < "$modules.sorted"

goroot=$(go env GOROOT)
standard="$output/std@$(go env GOVERSION)"
for directory in "$goroot" "$goroot/.."; do
  if [ -f "$directory/LICENSE" ]; then
    mkdir -p "$standard"
    cp "$directory/LICENSE" "$standard/LICENSE"
    break
  fi
done
if [ ! -f "$standard/LICENSE" ]; then
  echo 'no license found for the Go standard library' >&2
  exit 1
fi
if [ -f "$goroot/PATENTS" ]; then
  cp "$goroot/PATENTS" "$standard/PATENTS"
fi
