#!/usr/bin/env bash
set -euo pipefail

scripts=$(cd "$(dirname "$0")" && pwd)
repository=$(cd "$scripts/../.." && pwd)
git_directory=$(git -C "$repository" rev-parse --absolute-git-dir)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
fixture="$scratch/release fixture"
mkdir -p "$fixture"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_failure() {
  if "$@" > "$scratch/stdout" 2> "$scratch/stderr"; then
    fail "expected rejection: $*"
  fi
  test -s "$scratch/stderr" || fail "missing error: $*"
}

check_notes() {
  cat > "$fixture/CHANGELOG.md" <<'EOF'
## [Unreleased]

### Added
- Future change.

## [1x2x3] - 2026-11-05

- Another version.

## [1.2.3] - 2026-11-04

### Added
- Released change.

## [1.2.2] - 2026-11-03

## [1.2.1] - 2026-11-02
EOF
  printf '   \n' >> "$fixture/CHANGELOG.md"
  (
    cd "$fixture"
    "$scripts/release-notes.sh" v1.2.3 > "$scratch/notes"
    printf '\n### Added\n- Released change.\n\n' > "$scratch/expected"
    cmp "$scratch/expected" "$scratch/notes"
    expect_failure "$scripts/release-notes.sh" v9.9.9
    expect_failure "$scripts/release-notes.sh" v1.2.2
    expect_failure "$scripts/release-notes.sh" v1.2.1
  )
  echo 'PASS: release notes select the exact section and reject missing or empty notes'
}

prepare_modules() {
  mkdir -p "$fixture/dependency" "$fixture/windows-dependency" "$fixture/cmd/server"
  cat > "$fixture/go.mod" <<'EOF'
module example.test/release

go 1.27.3

require (
  example.test/dependency v1.0.0
  example.test/windows-dependency v1.0.0
)

replace example.test/dependency => ./dependency
replace example.test/windows-dependency => ./windows-dependency
EOF
  printf 'module example.test/dependency\n\ngo 1.27.3\n' > "$fixture/dependency/go.mod"
  printf 'package dependency\n' > "$fixture/dependency/dependency.go"
  printf 'Synthetic license.\n' > "$fixture/dependency/LiCeNcE.txt"
  printf 'Synthetic notice.\n' > "$fixture/dependency/NOTICE"
  printf 'Synthetic copying terms.\n' > "$fixture/dependency/COPYING.txt"
  printf 'Synthetic patent terms.\n' > "$fixture/dependency/PATENTS"
  printf 'module example.test/windows-dependency\n\ngo 1.27.3\n' > "$fixture/windows-dependency/go.mod"
  printf 'package windowsdependency\n' > "$fixture/windows-dependency/dependency.go"
  printf 'Synthetic Windows license.\n' > "$fixture/windows-dependency/LICENSE"
  cat > "$fixture/cmd/server/main.go" <<'EOF'
package main

import _ "example.test/dependency"

func main() {}
EOF
  cat > "$fixture/cmd/server/platform_windows.go" <<'EOF'
package main

import _ "example.test/windows-dependency"
EOF
}

check_licenses() {
  prepare_modules
  (
    cd "$fixture"
    GOOS=linux GOARCH=amd64 sh "$scripts/third-party-licenses.sh" ./cmd/server "$scratch/linux licenses"
    for name in LiCeNcE.txt NOTICE COPYING.txt PATENTS; do
      cmp "dependency/$name" "$scratch/linux licenses/example.test/dependency@v1.0.0/$name"
    done
    test ! -e "$scratch/linux licenses/example.test/windows-dependency@v1.0.0"
    test -s "$scratch/linux licenses/std@$(go env GOVERSION)/LICENSE" || fail 'Go standard library license missing'
    GOOS=windows GOARCH=arm64 sh "$scripts/third-party-licenses.sh" ./cmd/server "$scratch/windows licenses"
    cmp windows-dependency/LICENSE "$scratch/windows licenses/example.test/windows-dependency@v1.0.0/LICENSE"
    rm dependency/LiCeNcE.txt dependency/NOTICE dependency/COPYING.txt dependency/PATENTS
    expect_failure env GOOS=linux GOARCH=amd64 sh "$scripts/third-party-licenses.sh" ./cmd/server "$scratch/missing"
    grep -F 'example.test/dependency@v1.0.0' "$scratch/stderr" > /dev/null
    expect_failure sh "$scripts/third-party-licenses.sh" ./cmd/absent "$scratch/absent"
  )
  echo 'PASS: target-specific licenses are copied verbatim; missing licenses and packages fail'
}

check_archives() {
  prepare_modules
  mkdir -p "$fixture/cmd/quarry-mcp" "$fixture/cmd/utility" "$fixture/cmd/library" "$fixture/.github/scripts"
  cp "$fixture/cmd/server/main.go" "$fixture/cmd/quarry-mcp/main.go"
  cp "$fixture/cmd/server/main.go" "$fixture/cmd/utility/main.go"
  printf 'package library\n' > "$fixture/cmd/library/library.go"
  printf 'Synthetic project license.\n' > "$fixture/LICENSE"
  printf 'Synthetic readme.\n' > "$fixture/README.md"
  cp "$scripts/third-party-licenses.sh" "$fixture/.github/scripts/"
  (
    cd "$fixture"
    for tag in 1.0 v1.2.3+build v1.2.3/unsafe; do
      expect_failure env GIT_DIR="$git_directory" "$scripts/build-release.sh" "$tag" "$scratch/invalid"
      grep -Fx "invalid release tag: $tag" "$scratch/stderr" > /dev/null || fail "tag accepted: $tag"
    done
    GIT_DIR="$git_directory" "$scripts/build-release.sh" v1.2.3-check "$scratch/archives"
    cd "$scratch/archives"
    if command -v sha256sum > /dev/null 2>&1; then
      sha256sum -c quarry-rag_1.2.3-check_checksums.txt
    else
      shasum -a 256 -c quarry-rag_1.2.3-check_checksums.txt
    fi
    test "$(wc -l < quarry-rag_1.2.3-check_checksums.txt | tr -d ' ')" = 16
    for binary in quarry-server quarry-mcp quarry-utility; do
      for os in linux darwin windows; do
        for arch in amd64 arm64; do
          root="${binary}_1.2.3-check_${os}_${arch}"
          if [[ $binary == quarry-server && $os == windows ]]; then
            test ! -e "$root.zip" || fail 'server has a Windows archive'
            continue
          fi
          if [[ $os == windows ]]; then
            unzip -Z1 "$root.zip" > "$scratch/listing"
            grep -Fx "$root/$binary.exe" "$scratch/listing" > /dev/null
          else
            tar -tzf "$root.tar.gz" > "$scratch/listing"
            grep -Fx "$root/$binary" "$scratch/listing" > /dev/null
          fi
          grep -Fx "$root/LICENSE" "$scratch/listing" > /dev/null
          grep -Fx "$root/README.md" "$scratch/listing" > /dev/null
          grep -Fx "$root/licenses/example.test/dependency@v1.0.0/LiCeNcE.txt" "$scratch/listing" > /dev/null
          if grep -v "^$root/" "$scratch/listing"; then
            fail 'archive contains another top-level directory'
          fi
        done
      done
    done
  )
  echo 'PASS: command discovery, target matrix, archive contents, names and checksums'
}

check_discovery_failure() {
  prepare_modules
  mkdir -p "$fixture/cmd/broken" "$fixture/.github/scripts"
  printf 'invalid Go source\n' > "$fixture/cmd/broken/main.go"
  printf 'Synthetic project license.\n' > "$fixture/LICENSE"
  printf 'Synthetic readme.\n' > "$fixture/README.md"
  cp "$scripts/third-party-licenses.sh" "$fixture/.github/scripts/"
  (
    cd "$fixture"
    expect_failure env GIT_DIR="$git_directory" "$scripts/build-release.sh" v1.2.3-check "$scratch/discovery"
    grep -F 'cmd/broken' "$scratch/stderr" > /dev/null
  )
  echo 'PASS: command discovery errors stop packaging'
}

check_clean_builds() {
  local fixture="$scratch/clean fixture"
  mkdir -p "$fixture/cmd/quarry-mcp" "$fixture/.github/scripts"
  prepare_modules
  cp "$fixture/cmd/server/main.go" "$fixture/cmd/quarry-mcp/main.go"
  printf 'Synthetic project license.\n' > "$fixture/LICENSE"
  printf 'Synthetic readme.\n' > "$fixture/README.md"
  cp "$scripts/third-party-licenses.sh" "$fixture/.github/scripts/"
  (
    cd "$fixture"
    git init -q
    git add -A
    git -c user.name=release -c user.email=release@example.test -c commit.gpgsign=false commit -q -m fixture
    "$scripts/build-release.sh" v1.2.3-check dist
    mkdir "$scratch/extracted"
    for archive in dist/*.tar.gz; do tar -xzf "$archive" -C "$scratch/extracted"; done
    for archive in dist/*.zip; do unzip -q "$archive" -d "$scratch/extracted"; done
    for binary in "$scratch"/extracted/*/quarry-*; do
      go version -m "$binary" | grep -Fx $'\tbuild\tvcs.modified=false' > /dev/null || fail "modified build: $binary"
    done
    if tar --version 2> /dev/null | grep -q 'GNU tar'; then
      mv dist "$scratch/first build"
      sleep 1
      "$scripts/build-release.sh" v1.2.3-check dist
      cmp "$scratch/first build/quarry-rag_1.2.3-check_checksums.txt" dist/quarry-rag_1.2.3-check_checksums.txt || fail 'archives differ between builds'
    fi
  )
  echo 'PASS: archives written inside the checkout leave every build unmodified and reproducible'
}

case ${1:-all} in
  notes) check_notes ;;
  licenses) check_licenses ;;
  archives) check_archives ;;
  clean) check_clean_builds ;;
  discovery) check_discovery_failure ;;
  all) check_notes; check_licenses; check_archives; check_clean_builds; check_discovery_failure ;;
  *) fail 'usage: release-test.sh [notes|licenses|archives|clean|discovery|all]' ;;
esac
