#!/usr/bin/env bash
# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

[[ $# -eq 1 ]] || {
    echo "usage: $0 VERSION" >&2
    exit 2
}

version=$1
package_name=alternator-client

[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || {
    echo "version must be an exact X.Y.Z release without leading zeroes" >&2
    exit 1
}
[[ "${GITHUB_REF:-}" == refs/heads/main ]] || {
    echo "release workflow must be dispatched from main" >&2
    exit 1
}
commit_sha=$(git rev-parse HEAD)
[[ -n "${GITHUB_SHA:-}" && "$(git show -s --format=%P "$commit_sha")" == "$GITHUB_SHA" ]] || {
    echo "release commit must be a direct child of the dispatch commit" >&2
    exit 1
}
python3 "$(dirname "${BASH_SOURCE[0]}")/signing.py" verify-commit "$commit_sha"

git fetch --force --tags origin '+refs/heads/main:refs/remotes/origin/main'
main_sha=$(git rev-parse refs/remotes/origin/main)
current_run_rc=
while IFS= read -r tag; do
    if git cat-file tag "$tag" 2>/dev/null | grep -Fqx "workflow-run: ${GITHUB_RUN_ID:-}"; then
        [[ -z "$current_run_rc" ]] || {
            echo "multiple RC tags belong to workflow run $GITHUB_RUN_ID" >&2
            exit 1
        }
        current_run_rc=$tag
    fi
done < <(git tag -l "v$version-rc.*")

if [[ -n "$current_run_rc" ]]; then
    [[ "$(git rev-list -n 1 "$current_run_rc")" == "$commit_sha" ]] || {
        echo "$current_run_rc does not point to this workflow's commit" >&2
        exit 1
    }
else
    [[ "$GITHUB_SHA" == "$main_sha" ]] || {
        echo "workflow SHA $GITHUB_SHA is not the current origin/main commit $main_sha" >&2
        exit 1
    }
fi

metadata=$(cargo metadata --locked --no-deps --format-version 1)
actual_name=$(jq -er '.packages[0].name' <<<"$metadata")
actual_version=$(jq -er '.packages[0].version' <<<"$metadata")
[[ "$actual_name" == "$package_name" ]] || {
    echo "expected package $package_name, found $actual_name" >&2
    exit 1
}
[[ "$actual_version" == "$version" ]] || {
    echo "Cargo.toml version $actual_version does not match input $version" >&2
    exit 1
}

lock_version=$(awk -v wanted="$package_name" '
    /^\[\[package\]\]$/ { name = ""; version = "" }
    /^name = / { value = $0; sub(/^name = "/, "", value); sub(/"$/, "", value); name = value }
    /^version = / && name == wanted {
        value = $0; sub(/^version = "/, "", value); sub(/"$/, "", value); print value; exit
    }
' Cargo.lock)
[[ "$lock_version" == "$version" ]] || {
    echo "Cargo.lock version $lock_version does not match input $version" >&2
    exit 1
}

awk -v prefix="## [$version] - " '
    index($0, prefix) == 1 {
        count += 1
        date = substr($0, length(prefix) + 1)
        if (date ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) valid += 1
    }
    END { exit count == 1 && valid == 1 ? 0 : 1 }
' CHANGELOG.md || {
    echo "CHANGELOG.md has no dated [$version] release entry" >&2
    exit 1
}

if git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
    if [[ -z "$current_run_rc" || "$(git rev-list -n 1 "v$version")" != "$commit_sha" ]]; then
        echo "final tag v$version already exists outside this run's recovery state" >&2
        exit 1
    fi
fi

index_path="${package_name:0:2}/${package_name:2:2}/$package_name"
index_body=$(mktemp)
index_status=$(curl -A "$package_name-release-workflow" -sS \
    -o "$index_body" -w '%{http_code}' "https://index.crates.io/$index_path")
if [[ "$index_status" == 200 ]] && jq -e --arg version "$version" 'select(.vers == $version)' "$index_body" >/dev/null; then
    published_records=$(jq -c --arg version "$version" 'select(.vers == $version)' "$index_body")
    published_count=$(printf '%s\n' "$published_records" | awk 'NF { count++ } END { print count + 0 }')
    [[ "$published_count" -eq 1 ]] || {
        echo "crates.io sparse index contains duplicate $package_name $version records" >&2
        exit 1
    }
    published_yanked=$(jq -er \
        'if (.yanked | type) == "boolean" then (.yanked | tostring) else error("invalid yanked field") end' \
        <<<"$published_records")
    [[ "$published_yanked" == false ]] || {
        echo "$package_name $version is yanked; refusing release recovery" >&2
        exit 1
    }
    [[ -n "$current_run_rc" ]] || {
        echo "$package_name $version is already published" >&2
        exit 1
    }
elif [[ "$index_status" == 200 && "$version" == 1.0.0 ]]; then
    echo "$package_name is already claimed on crates.io; local 1.0.0 bootstrap is unsafe" >&2
    exit 1
elif [[ "$index_status" == 404 && "$version" != 1.0.0 ]]; then
    echo "$package_name is unclaimed; the initial release must be 1.0.0" >&2
    exit 1
elif [[ "$index_status" != 200 && "$index_status" != 404 ]]; then
    echo "crates.io sparse index preflight returned HTTP $index_status" >&2
    exit 1
fi

echo "release preflight passed for $package_name $version at $commit_sha"
