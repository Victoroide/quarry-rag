#!/usr/bin/env bash
set -euo pipefail
export GOWORK=off

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
  local go_version
  go_version=$(go -C "$repository" list -m -f '{{.GoVersion}}')

  mkdir -p "$fixture/dependency" "$fixture/windows-dependency" "$fixture/cmd/server"
  cat > "$fixture/go.mod" <<EOF
module example.test/release

go $go_version

require (
  example.test/dependency v1.0.0
  example.test/windows-dependency v1.0.0
)

replace example.test/dependency => ./dependency
replace example.test/windows-dependency => ./windows-dependency
EOF
  printf 'module example.test/dependency\n\ngo %s\n' "$go_version" > "$fixture/dependency/go.mod"
  printf 'package dependency\n' > "$fixture/dependency/dependency.go"
  printf 'Synthetic license.\n' > "$fixture/dependency/LiCeNcE.txt"
  printf 'Synthetic notice.\n' > "$fixture/dependency/NOTICE"
  printf 'Synthetic copying terms.\n' > "$fixture/dependency/COPYING.txt"
  printf 'Synthetic patent terms.\n' > "$fixture/dependency/PATENTS"
  printf 'module example.test/windows-dependency\n\ngo %s\n' "$go_version" > "$fixture/windows-dependency/go.mod"
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
    printf 'Synthetic embedded data license.\n' > cmd/server/LICENSE-UNICODE
    GOOS=linux GOARCH=amd64 sh "$scripts/third-party-licenses.sh" ./cmd/server "$scratch/linux licenses"
    cmp cmd/server/LICENSE-UNICODE "$scratch/linux licenses/example.test/release/cmd/server/LICENSE-UNICODE"
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
  cp "$fixture/cmd/server/platform_windows.go" "$fixture/cmd/quarry-mcp/platform_windows.go"
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

check_targets() {
  (
    cd "$fixture"
    tag=v1.2.3-check
    GIT_DIR="$git_directory" "$scripts/build-release.sh" --list "$tag" > "$scratch/expected-archives"
    cut -f1 "$scratch/expected-archives" | sort > "$scratch/expected-names"
    cut -d ' ' -f3 "$scratch/archives/quarry-rag_1.2.3-check_checksums.txt" | sort > "$scratch/default-names"
    cmp "$scratch/expected-names" "$scratch/default-names"

    for target in '' linux linux/386 plan9/amd64 linux/amd64/extra '../linux/amd64' 'linux/*' $'linux/amd64\nwindows/amd64' ' linux/amd64'; do
      expect_failure env GIT_DIR="$git_directory" "$scripts/build-release.sh" "$tag" "$scratch/invalid" "$target"
      grep -F 'invalid release target' "$scratch/stderr" > /dev/null
    done
    expect_failure env GIT_DIR="$git_directory" "$scripts/build-release.sh" "$tag" "$scratch/invalid" linux/amd64 linux/amd64
    grep -F 'duplicate release target' "$scratch/stderr" > /dev/null
    test ! -e "$scratch/invalid"

    GIT_DIR="$git_directory" "$scripts/build-release.sh" "$tag" "$scratch/first" linux/amd64 darwin/arm64 windows/amd64
    GIT_DIR="$git_directory" "$scripts/build-release.sh" "$tag" "$scratch/second" linux/arm64 darwin/amd64 windows/arm64
    for group in first second; do
      manifest="$scratch/$group/quarry-rag_1.2.3-check_checksums.txt"
      test "$(wc -l < "$manifest" | tr -d ' ')" = 8
      while read -r _ archive; do
        case "$group/$archive" in
          first/*_linux_amd64.tar.gz|first/*_darwin_arm64.tar.gz|first/*_windows_amd64.zip) ;;
          second/*_linux_arm64.tar.gz|second/*_darwin_amd64.tar.gz|second/*_windows_arm64.zip) ;;
          *) fail "unexpected target in $group: $archive" ;;
        esac
        case "$archive" in
          *.zip) cmp "$scratch/$group/$archive" "$scratch/archives/$archive" ;;
          *)
            mkdir -p "$scratch/full-extracted" "$scratch/group-extracted"
            tar -xzf "$scratch/archives/$archive" -C "$scratch/full-extracted"
            tar -xzf "$scratch/$group/$archive" -C "$scratch/group-extracted"
            ;;
        esac
      done < "$manifest"
    done
    diff -r "$scratch/full-extracted" "$scratch/group-extracted"

    mkdir cmd/quarry-utility
    cp cmd/utility/main.go cmd/quarry-utility/
    expect_failure env GIT_DIR="$git_directory" "$scripts/build-release.sh" --list "$tag"
    grep -F 'duplicate release command' "$scratch/stderr" > /dev/null
    rm -r cmd/quarry-utility
  )
  echo 'PASS: target selection matches the default archives and rejects invalid or duplicate entries'
}

check_preflight() {
  (
    cd "$fixture"
    tag=v1.2.3-check
    checksum=quarry-rag_1.2.3-check_checksums.txt
    verify="$scripts/check-release.sh"
    first="$scratch/first/$checksum"
    second="$scratch/second/$checksum"
    expected="$scratch/expected-names"
    "$verify" archives "$tag" "$scratch/first" linux/amd64 darwin/arm64 windows/amd64
    "$verify" archives "$tag" "$scratch/second" linux/arm64 darwin/amd64 windows/arm64
    GOEXPERIMENT=invalid "$verify" inventory "$expected" "$first" "$second"
    expect_failure "$verify" inventory "$expected" "$first"
    expect_failure "$verify" inventory "$expected" "$first" "$second" "$first"
    : > "$scratch/empty"
    expect_failure "$verify" inventory "$expected" "$first" "$scratch/empty"
    expect_failure "$verify" inventory "$scratch/empty" "$first" "$second"
    printf '%064d  unexpected_1.2.3-check_linux_amd64.tar.gz\n' 0 > "$scratch/extra"
    expect_failure "$verify" inventory "$expected" "$first" "$second" "$scratch/extra"
    first_line=$(head -n 1 "$first")
    RANDOM=197
    for _ in {1..12}; do
      offset=$((RANDOM % 64))
      printf '%sg%s\n' "${first_line:0:offset}" "${first_line:offset+1}" > "$scratch/malformed"
      expect_failure "$verify" inventory "$expected" "$scratch/malformed" "$second"
      grep -F 'invalid or empty checksum file' "$scratch/stderr" > /dev/null
    done
    for line in "${first_line/  / }" "${first_line/  /$'\t'}" "${first_line/  /  ../}" "$first_line "; do
      printf '%s\n' "$line" > "$scratch/malformed"
      expect_failure "$verify" inventory "$expected" "$scratch/malformed" "$second"
      grep -F 'invalid or empty checksum file' "$scratch/stderr" > /dev/null
    done

    cp -R "$scratch/first" "$scratch/verify"
    root=quarry-mcp_1.2.3-check_linux_amd64
    archive="$root.tar.gz"
    copy="$scratch/verify/$archive"
    selected=(linux/amd64 darwin/arm64 windows/amd64)

    unpack_original() {
      rm -rf "$scratch/corrupt"
      mkdir "$scratch/corrupt"
      tar -xzf "$scratch/first/$archive" -C "$scratch/corrupt"
    }

    repack_with_checksum() {
      tar -czf "$copy" -C "$scratch/corrupt" "$root"
      (
        cd "$scratch/verify"
        grep -Fv "  $archive" "$first" > "$checksum"
        if command -v sha256sum > /dev/null 2>&1; then
          sha256sum "$archive" >> "$checksum"
        else
          shasum -a 256 "$archive" >> "$checksum"
        fi
      )
    }

    rm "$copy"
    expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
    cp "$scratch/first/$archive" "$copy"
    cp "$copy" "$scratch/verify/extra.tar.gz"
    expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
    rm "$scratch/verify/extra.tar.gz"
    printf 'bad checksum\n' >> "$copy"
    expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
    cp "$scratch/first/$archive" "$copy"

    notice=licenses/example.test/dependency@v1.0.0/NOTICE
    for file in LICENSE README.md quarry-mcp licenses/std@"$(go env GOVERSION)"/LICENSE "$notice"; do
      unpack_original
      rm "$scratch/corrupt/$root/$file"
      repack_with_checksum
      expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
      grep -F "$file" "$scratch/stderr" > /dev/null || fail "missing file accepted: $file"
    done

    unpack_original
    printf 'wrong notice\n' > "$scratch/corrupt/$root/$notice"
    repack_with_checksum
    expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
    grep -F 'licenses differ' "$scratch/stderr" > /dev/null

    unpack_original
    other=quarry-mcp_1.2.3-check_linux_arm64
    tar -xOf "$scratch/second/$other.tar.gz" "$other/quarry-mcp" > "$scratch/corrupt/$root/quarry-mcp"
    repack_with_checksum
    expect_failure "$verify" archives "$tag" "$scratch/verify" "${selected[@]}"
    grep -F 'wrong executable' "$scratch/stderr" > /dev/null
  )
  echo 'PASS: shard verification rejects missing, duplicate, extra, malformed and defective archives or inventories'
}

check_inputs() {
  local inputs="$scratch/inputs"
  mkdir -p "$inputs/docs" "$inputs/internal/store/postgres/migrations" "$inputs/cmd/new-command"
  (
    cd "$inputs"
    git init -q
    record_files() {
      git add -A
      git -c user.name=release -c user.email=release@example.test -c commit.gpgsign=false commit -qm inputs
    }
    expect_inputs() {
      local result
      result=$("$scripts/release-inputs.sh" HEAD^ HEAD)
      test "$result" = "$1" || fail "expected preflight=$1, got $result"
    }

    touch docs/api.md README.md LICENSE
    record_files
    test "$("$scripts/release-inputs.sh" HEAD^ HEAD 2> "$scratch/stderr")" = true
    printf 'docs\n' > 'docs/user guide.md'
    record_files
    expect_inputs false
    printf 'embedded data\n' > internal/store/postgres/migrations/new.sql
    record_files
    expect_inputs true
    git mv internal/store/postgres/migrations/new.sql docs/old.md
    record_files
    expect_inputs true
    rm README.md
    record_files
    expect_inputs true
    printf 'new notice\n' > cmd/new-command/NOTICE
    record_files
    expect_inputs true
    printf 'updated license\n' > LICENSE
    record_files
    expect_inputs true
    printf 'unknown input\n' > 'some future input'
    record_files
    expect_inputs true
    printf 'package docs\n' > docs/assets.go
    record_files
    printf 'embedded docs\n' > docs/api.md
    record_files
    expect_inputs true
  )
  echo 'PASS: additions, renames, deletions and uncertain changes trigger preflight'
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
    rm -rf cmd/broken cmd/library cmd/quarry-mcp cmd/utility
    expect_failure "$scripts/build-release.sh" --list v1.2.3-check windows/amd64
    grep -F 'no archives for selected targets' "$scratch/stderr" > /dev/null
    rm -rf cmd/server
    expect_failure "$scripts/build-release.sh" --list v1.2.3-check
    grep -F 'no main packages found' "$scratch/stderr" > /dev/null
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
  archives) check_archives; check_targets ;;
  preflight) check_archives; check_targets; check_preflight ;;
  inputs) check_inputs ;;
  clean) check_clean_builds ;;
  discovery) check_discovery_failure ;;
  all) check_notes; check_licenses; check_archives; check_targets; check_preflight; check_inputs; check_clean_builds; check_discovery_failure ;;
  *) fail 'usage: release-test.sh [notes|licenses|archives|preflight|inputs|clean|discovery|all]' ;;
esac
