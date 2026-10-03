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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
checker=$script_dir/check-release-blockers.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
fake_bin=$test_dir/bin
mkdir "$fake_bin"

cat >"$fake_bin/gh" <<'FAKE_GH'
#!/usr/bin/env bash
set -euo pipefail

[[ "${1:-}" == api ]] || {
    echo "fake gh only supports the api command" >&2
    exit 90
}
[[ "${GH_TOKEN:-}" == test-token ]] || {
    echo "fake gh received an unexpected token" >&2
    exit 90
}

endpoint=${!#}
label_endpoint=repos/scylladb/alternator-client-rust/labels/release-blocker
issues_endpoint='repos/scylladb/alternator-client-rust/issues?state=open&labels=release-blocker&per_page=100'

if [[ "$endpoint" == "$label_endpoint" ]]; then
    case ${FAKE_GH_SCENARIO:?FAKE_GH_SCENARIO is required} in
        missing_label)
            echo "gh: Not Found (HTTP 404)" >&2
            exit 1
            ;;
        wrong_label)
            printf '%s\n' '{"name":"Release-Blocker"}'
            ;;
        malformed_label)
            printf '%s\n' '{"name":'
            ;;
        *)
            printf '%s\n' '{"name":"release-blocker"}'
            ;;
    esac
    exit 0
fi

[[ "$endpoint" == "$issues_endpoint" ]] || {
    echo "fake gh received unexpected endpoint: $endpoint" >&2
    exit 90
}

paginate=false
slurp=false
for argument in "$@"; do
    [[ "$argument" == --paginate ]] && paginate=true
    [[ "$argument" == --slurp ]] && slurp=true
done
[[ "$paginate" == true && "$slurp" == true ]] || {
    echo "issues request must use --paginate --slurp" >&2
    exit 90
}

case ${FAKE_GH_SCENARIO:?FAKE_GH_SCENARIO is required} in
    no_blockers)
        printf '%s\n' '[[]]'
        ;;
    one_blocker)
        printf '%s\n' '[[{"number":119,"title":"Block the release","html_url":"https://github.example/issues/119"}]]'
        ;;
    multiple_blockers)
        printf '%s\n' '[[{"number":7,"title":"First blocker","html_url":"https://github.example/issues/7"},{"number":42,"title":"Second blocker","html_url":"https://github.example/issues/42"}]]'
        ;;
    paginated)
        printf '%s\n' '[[{"number":11,"title":"Page one","html_url":"https://github.example/issues/11"}],[{"number":12,"title":"Page two","html_url":"https://github.example/issues/12"}]]'
        ;;
    pull_requests)
        printf '%s\n' '[[{"number":21,"title":"A pull request","html_url":"https://github.example/pull/21","pull_request":{}}],[{"number":22,"title":"Another pull request","html_url":"https://github.example/pull/22","pull_request":{"url":"https://api.github.example/pulls/22"}}]]'
        ;;
    api_failure)
        echo "gh: service unavailable (HTTP 503)" >&2
        exit 1
        ;;
    partial_pagination)
        printf '%s\n' '[[{"number":31,"title":"Partial result","html_url":"https://github.example/issues/31"}]]'
        echo "gh: pagination failed" >&2
        exit 1
        ;;
    malformed_json)
        printf '%s\n' '[[{"number":1]'
        ;;
    malformed_shape)
        printf '%s\n' '{"number":1,"title":"Not pages","html_url":"https://github.example/issues/1"}'
        ;;
    malformed_entry)
        printf '%s\n' '[[{"number":1,"title":false,"html_url":"https://github.example/issues/1"}]]'
        ;;
    malformed_pull_request)
        printf '%s\n' '[[{"number":1,"title":"Bad PR marker","html_url":"https://github.example/pull/1","pull_request":true}]]'
        ;;
    empty_response)
        ;;
    *)
        echo "unknown fake-gh scenario: $FAKE_GH_SCENARIO" >&2
        exit 90
        ;;
esac
FAKE_GH
chmod 755 "$fake_bin/gh"

test_count=0
last_output=
last_status=0

run_checker() {
    local scenario=$1
    local output_file=$test_dir/output

    set +e
    FAKE_GH_SCENARIO=$scenario \
        GITHUB_REPOSITORY=scylladb/alternator-client-rust \
        GH_TOKEN=test-token \
        PATH="$fake_bin:$PATH" \
        "$checker" >"$output_file" 2>&1
    last_status=$?
    set -e
    last_output=$(<"$output_file")
}

expect_success() {
    local scenario=$1

    run_checker "$scenario"
    [[ "$last_status" -eq 0 ]] || {
        echo "$scenario: expected success, got status $last_status" >&2
        echo "$last_output" >&2
        exit 1
    }
    test_count=$((test_count + 1))
}

expect_failure() {
    local scenario=$1
    local diagnostic=$2

    run_checker "$scenario"
    [[ "$last_status" -ne 0 ]] || {
        echo "$scenario: expected failure" >&2
        echo "$last_output" >&2
        exit 1
    }
    [[ "$last_output" == *"$diagnostic"* ]] || {
        echo "$scenario: missing diagnostic: $diagnostic" >&2
        echo "$last_output" >&2
        exit 1
    }
    test_count=$((test_count + 1))
}

expect_success no_blockers
[[ "$last_output" == *"no open release-blocker issues"* ]] || {
    echo "no_blockers: missing success diagnostic" >&2
    exit 1
}

expect_success pull_requests
[[ "$last_output" != *"#21"* && "$last_output" != *"#22"* ]] || {
    echo "pull_requests: pull requests were reported as blockers" >&2
    exit 1
}

expect_failure one_blocker '#119: Block the release (https://github.example/issues/119)'

expect_failure multiple_blockers '#7: First blocker (https://github.example/issues/7)'
[[ "$last_output" == *"#42: Second blocker (https://github.example/issues/42)"* ]] || {
    echo "multiple_blockers: second blocker was not reported" >&2
    exit 1
}

expect_failure paginated '#11: Page one (https://github.example/issues/11)'
[[ "$last_output" == *"#12: Page two (https://github.example/issues/12)"* ]] || {
    echo "paginated: blocker from the second page was not reported" >&2
    exit 1
}

expect_failure missing_label 'failed to verify the exact release-blocker label'
expect_failure wrong_label 'did not return the exact release-blocker label'
expect_failure malformed_label 'label API returned a malformed response'
expect_failure api_failure 'failed to query every page'
expect_failure partial_pagination 'failed to query every page'
expect_failure malformed_json 'empty or malformed paginated response'
expect_failure malformed_shape 'empty or malformed paginated response'
expect_failure malformed_entry 'empty or malformed paginated response'
expect_failure malformed_pull_request 'empty or malformed paginated response'
expect_failure empty_response 'empty or malformed paginated response'

output_file=$test_dir/contract-output
set +e
env -u GITHUB_REPOSITORY GH_TOKEN=test-token \
    "$checker" >"$output_file" 2>&1
last_status=$?
set -e
[[ "$last_status" -ne 0 && "$(<"$output_file")" == *"GITHUB_REPOSITORY is required"* ]] || {
    echo "missing-repository contract check failed" >&2
    exit 1
}
test_count=$((test_count + 1))

set +e
env -u GH_TOKEN GITHUB_REPOSITORY=scylladb/alternator-client-rust \
    "$checker" >"$output_file" 2>&1
last_status=$?
set -e
[[ "$last_status" -ne 0 && "$(<"$output_file")" == *"GH_TOKEN is required"* ]] || {
    echo "missing-token contract check failed" >&2
    exit 1
}
test_count=$((test_count + 1))

set +e
GITHUB_REPOSITORY=scylladb/alternator-client-rust GH_TOKEN=test-token \
    "$checker" unexpected >"$output_file" 2>&1
last_status=$?
set -e
[[ "$last_status" -eq 2 && "$(<"$output_file")" == *"usage:"* ]] || {
    echo "argument contract check failed" >&2
    exit 1
}
test_count=$((test_count + 1))

echo "$test_count release-blocker checker tests passed"
