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

[[ $# -eq 0 ]] || {
    echo "usage: $0" >&2
    exit 2
}

[[ -n "${GITHUB_REPOSITORY:-}" ]] || {
    echo "GITHUB_REPOSITORY is required" >&2
    exit 1
}
[[ -n "${GH_TOKEN:-}" ]] || {
    echo "GH_TOKEN is required" >&2
    exit 1
}
[[ "$GITHUB_REPOSITORY" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]] || {
    echo "GITHUB_REPOSITORY must be in owner/repository form" >&2
    exit 1
}

for required_command in gh jq; do
    command -v "$required_command" >/dev/null 2>&1 || {
        echo "$required_command is required" >&2
        exit 1
    }
done

label_name=release-blocker
api_version='X-GitHub-Api-Version: 2026-03-10'
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT
label_response=$work_dir/label.json
issues_response=$work_dir/issues.json

if ! gh api -H "$api_version" \
    "repos/$GITHUB_REPOSITORY/labels/$label_name" >"$label_response"; then
    echo "failed to verify the exact $label_name label" >&2
    exit 1
fi

if ! jq -se --arg label "$label_name" '
    length == 1
    and (.[0] | type == "object" and .name == $label)
' "$label_response" >/dev/null; then
    echo "label API returned a malformed response or did not return the exact $label_name label" >&2
    exit 1
fi

if ! gh api --paginate --slurp -H "$api_version" \
    "repos/$GITHUB_REPOSITORY/issues?state=open&labels=$label_name&per_page=100" \
    >"$issues_response"; then
    echo "failed to query every page of open $label_name issues" >&2
    exit 1
fi

if ! jq -se '
    def valid_entry:
        type == "object"
        and (.number | type == "number" and . > 0 and floor == .)
        and (.title | type == "string")
        and (.html_url | type == "string" and length > 0)
        and ((has("pull_request") | not) or (.pull_request | type == "object"));

    length == 1
    and (.[0] |
        type == "array"
        and length > 0
        and all(.[];
            type == "array"
            and all(.[]; valid_entry)))
' "$issues_response" >/dev/null; then
    echo "issues API returned an empty or malformed paginated response" >&2
    exit 1
fi

blocker_count=$(jq -er \
    '[.[][] | select(has("pull_request") | not)] | length' \
    "$issues_response")
if (( blocker_count > 0 )); then
    echo "release blocked by open issues carrying the exact $label_name label:" >&2
    jq -r '
        .[][]
        | select(has("pull_request") | not)
        | "#\(.number): \(.title) (\(.html_url))"
    ' "$issues_response" >&2
    exit 1
fi

echo "no open $label_name issues"
