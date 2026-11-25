#!/usr/bin/env bash
set -euo pipefail
export CGO_ENABLED=0 LC_ALL=C

fail() {
  echo "$*" >&2
  exit 1
}

if [[ $# -lt 3 || ( $1 != archives && $1 != inventory ) ]]; then
  fail 'usage: check-release.sh archives <tag> <output-dir> [os/arch ...] or check-release.sh inventory <expected-file> <checksum-file> ...'
fi
mode=$1
input=$2
shift 2
scripts=$(cd "$(dirname "$0")" && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

read_checksums() {
  local manifest
  for manifest in "$@"; do
    awk '
      length($1) != 64 || $1 ~ /[^0-9a-f]/ || substr($0, 65, 2) != "  " ||
        substr($0, 67) !~ /^[a-zA-Z0-9_.-]+\.(tar\.gz|zip)$/ { exit 1 }
      { print substr($0, 67) }
      END { if (NR == 0) exit 1 }
    ' "$manifest" || fail "invalid or empty checksum file: $manifest"
  done
}

compare_entries() {
  sort "$1" > "$stage/wanted"
  sort "$2" > "$stage/found"
  diff -u "$stage/wanted" "$stage/found" >&2 || fail "$3"
}

list_expected() {
  "$scripts/build-release.sh" --list "$tag" "$@" > "$stage/targets.tsv"
  cut -f1 "$stage/targets.tsv" > "$stage/expected-archives"
}

check_archive() {
  local archive=$1 package=$2 os=$3 arch=$4 binary=$5
  local file="$output/$archive"
  local root executable expected="$stage/expected" extracted="$stage/extracted"
  [[ -f $file && ! -L $file ]] || fail "expected a regular archive: $archive"
  case "$archive" in
    *.zip)
      root=${archive%.zip}
      executable="$binary.exe"
      unzip -Z1 "$file" > "$stage/archive-entries"
      ;;
    *.tar.gz)
      root=${archive%.tar.gz}
      executable=$binary
      tar -tzf "$file" > "$stage/archive-entries"
      ;;
  esac
  mkdir -p "$expected/$root" "$extracted"
  touch "$expected/$root/LICENSE" "$expected/$root/README.md" "$expected/$root/$executable"
  GOOS=$os GOARCH=$arch sh "$scripts/third-party-licenses.sh" "$package" "$expected/$root/licenses"
  (
    cd "$expected"
    find "$root" -type d -print | sed 's|$|/|'
    find "$root" -type f -print
  ) > "$stage/expected-entries"
  compare_entries "$stage/expected-entries" "$stage/archive-entries" "unexpected contents in $archive"

  case "$archive" in
    *.zip) unzip -q "$file" -d "$extracted" ;;
    *.tar.gz) tar -xzf "$file" -C "$extracted" ;;
  esac
  find "$extracted/$root" ! -type d ! -type f -print > "$stage/nonregular"
  [[ ! -s $stage/nonregular ]] || fail "non-regular entries in $archive"
  cmp -s "$project/LICENSE" "$extracted/$root/LICENSE" || fail "LICENSE differs in $archive"
  cmp -s "$project/README.md" "$extracted/$root/README.md" || fail "README.md differs in $archive"
  diff -qr "$expected/$root/licenses" "$extracted/$root/licenses" >&2 || fail "licenses differ in $archive"
  [[ $os == windows || -x $extracted/$root/$executable ]] || fail "missing execute permission in $archive"
  go version -m "$extracted/$root/$executable" > "$stage/build-info"
  if ! grep -Fxq $'\tpath\t'"$module/${package#./}" "$stage/build-info" ||
    ! grep -Fxq $'\tbuild\tGOOS='"$os" "$stage/build-info" ||
    ! grep -Fxq $'\tbuild\tGOARCH='"$arch" "$stage/build-info"; then
    fail "wrong executable in $archive"
  fi
  rm -rf "$expected" "$extracted"
}

if [[ $mode == inventory ]]; then
  read_checksums "$@" > "$stage/checksum-entries"
  compare_entries "$input" "$stage/checksum-entries" 'combined archive inventory differs from expected targets'
  echo 'Combined archive inventory matches all release targets.'
else
  tag=$input
  output=$1
  shift
  manifest="quarry-rag_${tag#v}_checksums.txt"
  list_expected "$@"
  read_checksums "$output/$manifest" > "$stage/checksum-entries"
  compare_entries "$stage/expected-archives" "$stage/checksum-entries" 'checksum inventory differs from selected targets'
  project=$PWD
  module=$(go list -m -f '{{.Path}}')
  cd "$output"
  shopt -s nullglob
  printf '%s\n' *.tar.gz *.zip > "$stage/output-archives"
  compare_entries "$stage/expected-archives" "$stage/output-archives" 'output archives differ from selected targets'
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum -c "$manifest"
  else
    shasum -a 256 -c "$manifest"
  fi
  cd "$project"
  output=$(cd "$output" && pwd)
  while IFS=$'\t' read -r archive package os arch binary; do
    check_archive "$archive" "$package" "$os" "$arch" "$binary"
  done < "$stage/targets.tsv"
fi
