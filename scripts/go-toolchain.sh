#!/usr/bin/env bash

# resolve_go_patroni_go_command resolves a toolchain compatible with go.mod
# before callers switch to an isolated module cache or disable proxy access.
# The Go command itself enforces the module's minimum version.
resolve_go_patroni_go_command() {
  if (($# != 1)); then
    printf 'resolve_go_patroni_go_command requires the go-patroni repository root\n' >&2
    return 2
  fi

  local repo_root=$1
  local bootstrap=${GO:-go}
	local required_version go_root candidate
  if [[ ! -f $repo_root/go.mod ]]; then
    printf 'go-patroni go.mod does not exist under %s\n' "$repo_root" >&2
    return 1
  fi
  required_version=$(awk '$1 == "go" { print $2; exit }' "$repo_root/go.mod")
  if [[ -z $required_version ]]; then
    printf 'go-patroni go.mod does not declare a Go version\n' >&2
    return 1
  fi
  if ! go_root=$(cd "$repo_root" && GOTOOLCHAIN=auto "$bootstrap" env GOROOT); then
    printf 'cannot resolve the required Go %s toolchain\n' "$required_version" >&2
    return 1
  fi
  candidate="$go_root/bin/go"
  if [[ ! -x $candidate ]]; then
    printf 'resolved Go toolchain is not executable: %s\n' "$candidate" >&2
    return 1
  fi
	printf '%s\n' "$candidate"
}

# activate_go_patroni_go_toolchain also makes the compatible binary visible to
# child Go tools that resolve `go` from PATH.
activate_go_patroni_go_toolchain() {
  if (($# != 1)); then
    printf 'activate_go_patroni_go_toolchain requires the go-patroni repository root\n' >&2
    return 2
  fi
  GO_PATRONI_GO_COMMAND=$(resolve_go_patroni_go_command "$1") || return
  PATH="$(dirname "$GO_PATRONI_GO_COMMAND"):$PATH"
  GOTOOLCHAIN=local
  export GO_PATRONI_GO_COMMAND PATH GOTOOLCHAIN
}
