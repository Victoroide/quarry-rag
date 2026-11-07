#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo 'usage: build-release.sh <tag> <output-dir>' >&2
  exit 1
fi
tag=$1
if [[ ! $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid release tag: $tag" >&2
  exit 1
fi

version=${tag#v}
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
export SOURCE_DATE_EPOCH CGO_ENABLED=0 LC_ALL=C
zip_time=$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S 2> /dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S)
mkdir -p "$2"
output=$(cd "$2" && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
packages="$stage/packages"
mkdir "$packages"
archives=()
gnu_tar=false
if tar --version 2>/dev/null | grep -q 'GNU tar'; then
  gnu_tar=true
fi

for directory in cmd/*; do
  [[ -d $directory ]] || continue
  compgen -G "$directory/*.go" > /dev/null || continue
  package="./$directory"
  package_name=$(go list -f '{{.Name}}' "$package")
  [[ $package_name == main ]] || continue
  name=${directory##*/}
  case "$name" in
    quarry-*) binary=$name ;;
    *) binary="quarry-$name" ;;
  esac
  targets=(linux darwin)
  if [[ $binary != quarry-server ]]; then
    targets+=(windows)
  fi
  for os in "${targets[@]}"; do
    for arch in amd64 arm64; do
      root="${binary}_${version}_${os}_${arch}"
      destination="$stage/$root"
      mkdir -p "$destination"
      executable=$binary
      if [[ $os == windows ]]; then
        executable+=.exe
      fi
      echo "Building $binary for $os/$arch"
      GOOS=$os GOARCH=$arch go build -trimpath -ldflags='-s -w -buildid=' -o "$destination/$executable" "$package"
      cp LICENSE README.md "$destination/"
      GOOS=$os GOARCH=$arch sh .github/scripts/third-party-licenses.sh "$package" "$destination/licenses"
      if [[ $os == windows ]]; then
        archive="$root.zip"
        TZ=UTC find "$destination" -exec touch -t "$zip_time" {} +
        (cd "$stage" && find "$root" | sort | TZ=UTC zip -q -X "$packages/$archive" -@)
      else
        archive="$root.tar.gz"
        if [[ $gnu_tar == true ]]; then
          tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner -C "$stage" -cf - "$root" | gzip -n > "$packages/$archive"
        else
          tar -C "$stage" -cf - "$root" | gzip -n > "$packages/$archive"
        fi
      fi
      archives+=("$archive")
      rm -rf "$destination"
    done
  done
done

if [[ ${#archives[@]} -eq 0 ]]; then
  echo 'no main packages found under cmd/' >&2
  exit 1
fi
mv "${archives[@]/#/$packages/}" "$output/"
(
  cd "$output"
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "${archives[@]}"
  else
    shasum -a 256 "${archives[@]}"
  fi
) > "$output/quarry-rag_${version}_checksums.txt"
