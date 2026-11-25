#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "$*" >&2
  exit 1
}

if [[ $# -lt 2 ]]; then
  fail 'usage: build-release.sh <tag> <output-dir> [os/arch ...] or build-release.sh --list <tag> [os/arch ...]'
fi
if [[ $1 == --list ]]; then
  output_dir=''
  tag=$2
else
  tag=$1
  output_dir=$2
  [[ -n $output_dir ]] || fail 'release output directory is empty'
fi
shift 2
if [[ ! $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  fail "invalid release tag: $tag"
fi

version=${tag#v}
export CGO_ENABLED=0 LC_ALL=C
platforms=(linux darwin windows)
architectures=(amd64 arm64)
supported=' '
for os in "${platforms[@]}"; do
  for arch in "${architectures[@]}"; do
    supported+="$os/$arch "
  done
done

selected=' '
for target in "$@"; do
  [[ $target =~ ^[a-z]+/[a-z0-9]+$ && $supported == *" $target "* ]] || fail "invalid release target: $target"
  [[ $selected != *" $target "* ]] || fail "duplicate release target: $target"
  selected+="$target "
done
if [[ $# -eq 0 ]]; then
  selected=$supported
fi

list_archives() {
  local directory package package_name name binary os arch root extension
  local commands=' '
  for directory in cmd/*; do
    [[ -d $directory ]] || continue
    compgen -G "$directory/*.go" > /dev/null || continue
    package="./$directory"
    package_name=$(go list -f '{{.Name}}' "$package") || return 1
    [[ $package_name == main ]] || continue
    name=${directory##*/}
    case "$name" in
      quarry-*) binary=$name ;;
      *) binary="quarry-$name" ;;
    esac
    [[ $binary =~ ^[a-zA-Z0-9_.-]+$ ]] || fail "invalid release command: $binary"
    [[ $commands != *" $binary "* ]] || fail "duplicate release command: $binary"
    commands+="$binary "

    for os in "${platforms[@]}"; do
      extension=tar.gz
      if [[ $os == windows ]]; then
        [[ $binary != quarry-server ]] || continue
        extension=zip
      fi
      for arch in "${architectures[@]}"; do
        [[ $selected == *" $os/$arch "* ]] || continue
        root="${binary}_${version}_${os}_${arch}"
        printf '%s\t%s\t%s\t%s\t%s\n' "$root.$extension" "$package" "$os" "$arch" "$binary"
      done
    done
  done
  [[ $commands != ' ' ]] || fail 'no main packages found under cmd/'
}

inventory=$(list_archives)
[[ -n $inventory ]] || fail 'no archives for selected targets'
if [[ -z $output_dir ]]; then
  printf '%s\n' "$inventory"
  exit 0
fi

SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
export SOURCE_DATE_EPOCH
zip_time=$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S 2> /dev/null || date -u -r "$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S)
mkdir -p "$output_dir"
output=$(cd "$output_dir" && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
packages="$stage/packages"
mkdir "$packages"
archives=()
gnu_tar=false
if tar --version 2>/dev/null | grep -q 'GNU tar'; then
  gnu_tar=true
fi

# Share compiler objects within each target and keep the discovery order in the checksum file.
build_inventory=$(printf '%s\n' "$inventory" | sort -s -k3,3 -k4,4)
while IFS=$'\t' read -r archive package os arch binary; do
  case "$archive" in
    *.zip) root=${archive%.zip} ;;
    *) root=${archive%.tar.gz} ;;
  esac
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
    TZ=UTC find "$destination" -exec touch -t "$zip_time" {} +
    (cd "$stage" && find "$root" | sort | TZ=UTC zip -q -X "$packages/$archive" -@)
  elif [[ $gnu_tar == true ]]; then
    tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner -C "$stage" -cf - "$root" | gzip -n > "$packages/$archive"
  else
    tar -C "$stage" -cf - "$root" | gzip -n > "$packages/$archive"
  fi
  rm -rf "$destination"
done <<< "$build_inventory"

while IFS=$'\t' read -r archive _; do
  archives+=("$archive")
done <<< "$inventory"
mv "${archives[@]/#/$packages/}" "$output/"
(
  cd "$output"
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "${archives[@]}"
  else
    shasum -a 256 "${archives[@]}"
  fi
) > "$output/quarry-rag_${version}_checksums.txt"
