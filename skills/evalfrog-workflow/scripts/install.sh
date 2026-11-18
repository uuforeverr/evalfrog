#!/usr/bin/env bash
set -euo pipefail

skill_name='evalfrog-workflow'
force=0
destination_root=''

while [ "$#" -gt 0 ]; do
  case "$1" in
    --force) force=1 ;;
    --destination-root)
      shift
      [ "$#" -gt 0 ] || { echo '--destination-root requires a path' >&2; exit 2; }
      destination_root="$1"
      ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
skill_root=$(CDPATH= cd -- "$script_directory/.." && pwd -P)
if [ -z "$destination_root" ]; then
  user_home_directory=${HOME:?HOME is required when --destination-root is omitted}
  codex_home_directory=${CODEX_HOME:-"$user_home_directory/.codex"}
  destination_root="$codex_home_directory/skills"
fi

mkdir -p -- "$destination_root"
destination_root=$(CDPATH= cd -- "$destination_root" && pwd -P)
destination_skill="$destination_root/$skill_name"

[ "$destination_skill" != "$skill_root" ] || { echo 'source and destination must differ' >&2; exit 2; }
if [ -e "$destination_skill" ]; then
  [ "$force" -eq 1 ] || { echo "skill already exists at $destination_skill; rerun with --force to replace it" >&2; exit 1; }
  [ "$(basename -- "$destination_skill")" = "$skill_name" ] || { echo 'refusing to remove an unexpected destination' >&2; exit 2; }
  rm -rf -- "$destination_skill"
fi

cp -R -- "$skill_root" "$destination_root/"
printf 'installed %s to %s\n' "$skill_name" "$destination_skill"
