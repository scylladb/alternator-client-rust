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

[[ $# -eq 2 ]] || {
    echo "usage: $0 VERSION COMMIT_SHA" >&2
    exit 2
}

version=$1
commit_sha=$2
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
run_id=${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}
run_attempt=${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}

git fetch --force origin --tags

matched_tag=
while IFS= read -r tag; do
    if git cat-file tag "$tag" 2>/dev/null | grep -Fqx "workflow-run: $run_id"; then
        [[ -z "$matched_tag" ]] || {
            echo "more than one RC tag belongs to workflow run $run_id" >&2
            exit 1
        }
        matched_tag=$tag
    fi
done < <(git tag -l "v$version-rc.*" --sort=version:refname)

if [[ -n "$matched_tag" ]]; then
    tagged_commit=$(git rev-list -n 1 "$matched_tag")
    [[ "$tagged_commit" == "$commit_sha" ]] || {
        echo "$matched_tag belongs to this run but points to $tagged_commit" >&2
        exit 1
    }
    rc_tag=$matched_tag
else
    max_rc=0
    while IFS= read -r tag; do
        suffix=${tag##*.}
        [[ "$suffix" =~ ^[1-9][0-9]*$ ]] || continue
        number=$((10#$suffix))
        (( number > max_rc )) && max_rc=$number
    done < <(git tag -l "v$version-rc.*")
    rc_number=$((max_rc + 1))
    rc_tag="v$version-rc.$rc_number"

    git config user.name "github-actions[bot]"
    git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
    bash "$script_dir/check-release-blockers.sh"
    git tag -a "$rc_tag" "$commit_sha" -m "Release candidate $rc_tag

workflow-run: $run_id
workflow-attempt-created: $run_attempt
commit: $commit_sha"
    git push origin "refs/tags/$rc_tag"
fi

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "rc_tag=$rc_tag" >>"$GITHUB_OUTPUT"
fi

echo "$rc_tag"
