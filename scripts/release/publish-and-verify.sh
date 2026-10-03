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

[[ $# -eq 3 ]] || {
    echo "usage: $0 PACKAGE_DIR VERSION CANDIDATE_CRATE" >&2
    exit 2
}

package_dir=$1
version=$2
candidate_crate=$3
package_name=alternator-client
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

[[ -n "${CARGO_REGISTRY_TOKEN:-}" ]] || {
    echo "trusted-publishing token is missing" >&2
    exit 1
}

bash "$script_dir/check-release-blockers.sh"

set +e
(
    cd "$package_dir"
    # The candidate already passed cargo package verification before this
    # short-lived token was requested. Avoid exposing it to build scripts.
    publish_target=$(mktemp -d)
    cargo publish --locked --no-verify --target-dir "$publish_target"
)
publish_status=$?
set -e

for attempt in $(seq 1 30); do
    state_output=$(mktemp)
    if GITHUB_OUTPUT="$state_output" scripts/release/registry-state.sh \
        "$package_name" "$version" "$candidate_crate" >/dev/null; then
        state=$(awk -F= '$1 == "state" { print $2 }' "$state_output")
        if [[ "$state" == exact ]]; then
            registry_sha=$(awk -F= '$1 == "registry_sha256" { print $2 }' "$state_output")
            if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
                echo "registry_sha256=$registry_sha" >>"$GITHUB_OUTPUT"
            fi
            echo "crates.io serves the exact tested candidate ($registry_sha)"
            exit 0
        fi
    else
        state=$(awk -F= '$1 == "state" { print $2 }' "$state_output")
        if [[ "$state" == conflict ]]; then
            echo "registry verification found a conflicting version" >&2
            exit 1
        fi
        echo "registry metadata or bytes are not fully available yet"
    fi
    echo "waiting for crates.io propagation ($attempt/30)"
    sleep 10
done

if [[ "$publish_status" -ne 0 ]]; then
    echo "cargo publish failed with status $publish_status and the version is absent" >&2
else
    echo "cargo publish returned success but the version did not propagate in time" >&2
fi
exit 1
