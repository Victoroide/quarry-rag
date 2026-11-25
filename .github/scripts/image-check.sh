#!/bin/sh
set -eu

if [ "$#" -lt 1 ]; then
  echo 'usage: image-check.sh <oci-archive> [linux/amd64|linux/arm64 ...]' >&2
  exit 2
fi
archive=$1
case "$archive" in
  /*) ;;
  *) archive=$PWD/$archive ;;
esac
shift
if [ "$#" -eq 0 ]; then
  set -- linux/amd64 linux/arm64
fi
for platform in "$@"; do
  case "$platform" in
    linux/amd64|linux/arm64) ;;
    *) echo "image-check: unsupported platform: $platform" >&2; exit 2 ;;
  esac
done
if [ ! -r "$archive" ]; then
  echo "image-check: cannot read $archive" >&2
  exit 1
fi

cd "$(dirname "$0")/../.."
notices=$(mktemp -d)
trap 'rm -rf "$notices"' EXIT
for platform in "$@"; do
  CGO_ENABLED=0 GOOS=${platform%/*} GOARCH=${platform#*/} \
    sh .github/scripts/third-party-licenses.sh ./cmd/server "$notices/${platform%/*}_${platform#*/}"
done

go run ./tools/imagecheck -license LICENSE -notices "$notices" "$archive" "$@"
