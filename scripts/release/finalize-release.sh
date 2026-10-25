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

[[ $# -eq 6 && ( "$1" == prepare || "$1" == publish ) ]] || {
    echo "usage: $0 {prepare|publish} VERSION RC_TAG COMMIT_SHA CANDIDATE_DIR EVIDENCE_FILE" >&2
    exit 2
}

phase=$1
version=$2
rc_tag=$3
commit_sha=$4
candidate_dir=$5
evidence_file=$6
package_name=alternator-client
final_tag="v$version"
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for command in curl gh git jq shasum; do
    command -v "$command" >/dev/null || {
        echo "required command is missing: $command" >&2
        exit 1
    }
done
[[ -n "${GITHUB_REPOSITORY:-}" ]] || {
    echo "GITHUB_REPOSITORY is missing" >&2
    exit 1
}

manifest="$candidate_dir/release-manifest.json"
crate_file=$(jq -er '.crate.file' "$manifest")
sbom_file=$(jq -er '.sbom.file' "$manifest")
scripts/release/verify-candidate.sh "$candidate_dir" "$(mktemp -d)" \
    "$version" "$rc_tag" "$commit_sha" >/dev/null
[[ -f "$evidence_file" ]] || {
    echo "test evidence is missing: $evidence_file" >&2
    exit 1
}
jq -e --arg rc_tag "$rc_tag" --arg commit "$commit_sha" '
    .rc_tag == $rc_tag and .commit_sha == $commit and .all_required_gates_passed == true
' "$evidence_file" >/dev/null

state_file=$(mktemp)
GITHUB_OUTPUT="$state_file" scripts/release/registry-state.sh \
    "$package_name" "$version" "$candidate_dir/$crate_file" >/dev/null
registry_state=$(awk -F= '$1 == "state" { print $2 }' "$state_file")
if [[ "$phase" == prepare && "$registry_state" != absent ]]; then
    echo "registry version is already published; use publish to verify the existing draft" >&2
    exit 1
fi
[[ "$phase" == prepare || "$registry_state" == exact ]] || {
    echo "crates.io does not serve the tested candidate; refusing to finalize" >&2
    exit 1
}

git fetch --force origin --tags
python3 "$script_dir/signing.py" verify-tag "$rc_tag"
[[ "$(git rev-list -n 1 "$rc_tag")" == "$commit_sha" ]] || {
    echo "RC tag no longer resolves to the manifest commit" >&2
    exit 1
}

if git rev-parse -q --verify "refs/tags/$final_tag" >/dev/null; then
    [[ "$(git cat-file -t "refs/tags/$final_tag")" == tag ]] || {
        echo "existing final tag is not annotated" >&2
        exit 1
    }
    [[ "$(git rev-list -n 1 "$final_tag")" == "$commit_sha" ]] || {
        echo "existing final tag points to a different commit" >&2
        exit 1
    }
    python3 "$script_dir/signing.py" verify-tag "$final_tag"
    remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
    [[ "$remote_final_commit" == "$commit_sha" ]] || {
        echo "remote final tag does not point to the manifest commit" >&2
        exit 1
    }
fi

notes_file=$(mktemp)
awk -v version="$version" '
    index($0, "## [" version "] - ") == 1 { found = 1; next }
    found && /^## / { exit }
    found { print }
' CHANGELOG.md >"$notes_file"
[[ -s "$notes_file" ]] || {
    echo "could not extract release notes for $version" >&2
    exit 1
}

release_list=$(mktemp)
gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$GITHUB_REPOSITORY/releases?per_page=100" \
    | jq --arg tag "$final_tag" '[.[].[] | select(.tag_name == $tag)]' >"$release_list"
release_count=$(jq 'length' "$release_list")
[[ "$release_count" -le 1 ]] || {
    echo "multiple GitHub Releases use tag $final_tag" >&2
    exit 1
}

if [[ "$release_count" -eq 0 ]]; then
    [[ "$phase" == prepare ]] || {
        echo "verified draft GitHub Release is missing; refusing to finalize" >&2
        exit 1
    }
    bash "$script_dir/check-release-blockers.sh"
    # A draft may name a future tag. Create the signed final tag only after
    # crates.io serves the tested bytes, immediately before publishing.
    gh release create "$final_tag" --repo "$GITHUB_REPOSITORY" --target "$commit_sha" --draft \
        --title "$package_name v$version" --notes-file "$notes_file"
    gh api --paginate --slurp -H 'X-GitHub-Api-Version: 2026-03-10' \
        "repos/$GITHUB_REPOSITORY/releases?per_page=100" \
        | jq --arg tag "$final_tag" '[.[].[] | select(.tag_name == $tag)]' >"$release_list"
    [[ "$(jq 'length' "$release_list")" -eq 1 ]] || {
        echo "new draft GitHub Release could not be recovered" >&2
        exit 1
    }
fi
release_body=$(mktemp)
jq '.[0]' "$release_list" >"$release_body"
release_id=$(jq -er '.id' "$release_body")

# Keep candidate bytes and the four-file candidate contract unchanged. Recover
# existing signatures before signing so an interrupted draft can be resumed.
release_assets=$(mktemp -d)
cp "$candidate_dir/"* "$release_assets/"
cp "$evidence_file" "$release_assets/test-evidence.json"
for asset in "$crate_file.asc" SHA256SUMS.asc "$sbom_file.asc" release-manifest.json.asc test-evidence.json.asc; do
    if jq -e --arg name "$asset" '.assets[] | select(.name == $name and .state == "uploaded")' "$release_body" >/dev/null; then
        gh release download "$final_tag" --repo "$GITHUB_REPOSITORY" --pattern "$asset" --dir "$release_assets"
    fi
done
if [[ "$phase" == publish ]]; then
    for signature in "$crate_file.asc" SHA256SUMS.asc "$sbom_file.asc" release-manifest.json.asc test-evidence.json.asc; do
        [[ -f "$release_assets/$signature" ]] || {
            echo "verified draft signature is missing: $signature" >&2
            exit 1
        }
    done
fi
python3 "$script_dir/signing.py" sign-files \
    "$release_assets/$crate_file" "$release_assets/SHA256SUMS" \
    "$release_assets/$sbom_file" "$release_assets/release-manifest.json" "$release_assets/test-evidence.json"

assets=("$crate_file" SHA256SUMS "$sbom_file" release-manifest.json test-evidence.json
        "$crate_file.asc" SHA256SUMS.asc "$sbom_file.asc" release-manifest.json.asc test-evidence.json.asc)
expected_names=$(printf '%s\n' "${assets[@]}" | LC_ALL=C sort)

if jq -e '.draft == false' "$release_body" >/dev/null; then
    [[ "$registry_state" == exact ]] || {
        echo "published GitHub Release has no matching registry publication" >&2
        exit 1
    }
    jq -e '.immutable == true' "$release_body" >/dev/null || {
        echo "published release is not immutable" >&2
        exit 1
    }

    published_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
    [[ "$published_names" == "$expected_names" ]] || {
        echo "published release assets differ from the canonical asset set" >&2
        exit 1
    }
    verify_dir=$(mktemp -d)
    gh release download "$final_tag" --repo "$GITHUB_REPOSITORY" --dir "$verify_dir"
    for asset in "${assets[@]}"; do
        source_file="$candidate_dir/$asset"
        [[ "$asset" == test-evidence.json ]] && source_file=$evidence_file
        [[ "$asset" == *.asc ]] && source_file="$release_assets/$asset"
        [[ -f "$verify_dir/$asset" ]] && cmp -s "$source_file" "$verify_dir/$asset" || {
            echo "published release asset $asset is missing or differs" >&2
            exit 1
        }
    done
    echo "immutable GitHub Release $final_tag already contains the canonical assets"
    exit 0
fi

expected_notes=$(cat "$notes_file")
actual_notes=$(jq -r '.body' "$release_body")
jq -e \
    --arg tag "$final_tag" \
    --arg name "$package_name v$version" \
    '.draft == true and .tag_name == $tag and .name == $name' \
    "$release_body" >/dev/null || {
    echo "existing draft release metadata differs from the canonical release" >&2
    exit 1
}
if ! git rev-parse -q --verify "refs/tags/$final_tag" >/dev/null; then
    jq -e --arg commit "$commit_sha" '.target_commitish == $commit' "$release_body" >/dev/null || {
        echo "draft release target differs from the verified candidate" >&2
        exit 1
    }
fi
[[ "$actual_notes" == "$expected_notes" ]] || {
    echo "existing draft release notes differ from CHANGELOG.md" >&2
    exit 1
}

unexpected_names=$(comm -23 \
    <(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort) \
    <(printf '%s\n' "$expected_names"))
[[ -z "$unexpected_names" ]] || {
    echo "draft release contains unexpected assets:" >&2
    printf '%s\n' "$unexpected_names" >&2
    exit 1
}

for asset in "${assets[@]}"; do
    source_file="$candidate_dir/$asset"
    [[ "$asset" == test-evidence.json ]] && source_file=$evidence_file
    [[ "$asset" == *.asc ]] && source_file="$release_assets/$asset"
    asset_id=$(jq -r --arg name "$asset" '.assets[] | select(.name == $name) | .id' "$release_body")
    if [[ -n "$asset_id" ]]; then
        asset_state=$(jq -r --arg name "$asset" '.assets[] | select(.name == $name) | .state' "$release_body")
        if [[ "$phase" == publish && "$asset_state" != uploaded ]]; then
            echo "verified draft asset $asset is not uploaded" >&2
            exit 1
        fi
        if [[ "$asset_state" == starter ]]; then
            # GitHub can leave a zero-byte `starter` placeholder after a 502.
            # It is not a published asset and must be removed to retry safely.
            gh api --method DELETE -H 'X-GitHub-Api-Version: 2026-03-10' \
                "repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
            gh release upload "$final_tag" --repo "$GITHUB_REPOSITORY" "$source_file"
        elif [[ "$asset_state" == uploaded ]]; then
            existing_file=$(mktemp)
            curl -fsSL \
                -H 'Accept: application/octet-stream' \
                -H "Authorization: Bearer $GH_TOKEN" \
                -H 'X-GitHub-Api-Version: 2026-03-10' \
                -o "$existing_file" \
                "https://api.github.com/repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
            cmp -s "$source_file" "$existing_file" || {
                echo "existing draft asset $asset differs; refusing to replace it" >&2
                exit 1
            }
        else
            echo "existing draft asset $asset has unexpected state $asset_state" >&2
            exit 1
        fi
    else
        [[ "$phase" == prepare ]] || {
            echo "verified draft asset is missing: $asset" >&2
            exit 1
        }
        gh release upload "$final_tag" --repo "$GITHUB_REPOSITORY" "$source_file"
    fi
done

release_body=$(mktemp)
gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
    "repos/$GITHUB_REPOSITORY/releases/$release_id" >"$release_body"
uploaded_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
[[ "$uploaded_names" == "$expected_names" ]] || {
    echo "draft release does not contain exactly the canonical assets" >&2
    exit 1
}
jq -e '[.assets[].state] | all(. == "uploaded")' "$release_body" >/dev/null || {
    echo "not every draft asset reached the uploaded state" >&2
    exit 1
}

for asset in "${assets[@]}"; do
    source_file="$candidate_dir/$asset"
    [[ "$asset" == test-evidence.json ]] && source_file=$evidence_file
    [[ "$asset" == *.asc ]] && source_file="$release_assets/$asset"
    asset_id=$(jq -er --arg name "$asset" '.assets[] | select(.name == $name) | .id' "$release_body")
    uploaded_file=$(mktemp)
    curl -fsSL \
        -H 'Accept: application/octet-stream' \
        -H "Authorization: Bearer $GH_TOKEN" \
        -H 'X-GitHub-Api-Version: 2026-03-10' \
        -o "$uploaded_file" \
        "https://api.github.com/repos/$GITHUB_REPOSITORY/releases/assets/$asset_id"
    cmp -s "$source_file" "$uploaded_file" || {
        echo "uploaded draft asset $asset differs from its canonical local file" >&2
        exit 1
    }
done

if [[ "$phase" == prepare ]]; then
    echo "GitHub draft $final_tag contains the verified canonical assets"
    exit 0
fi

if ! git rev-parse -q --verify "refs/tags/$final_tag" >/dev/null; then
    bash "$script_dir/check-release-blockers.sh"
    git tag -s "$final_tag" "$commit_sha" -m "Release $package_name $version

Promoted from: $rc_tag
crate-sha256: $(jq -er '.crate.sha256' "$manifest")
commit: $commit_sha"
    python3 "$script_dir/signing.py" verify-tag "$final_tag"
    git push origin "refs/tags/$final_tag"
fi
python3 "$script_dir/signing.py" verify-tag "$final_tag"

remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
[[ "$remote_final_commit" == "$commit_sha" ]] || {
    echo "remote final tag moved before immutable release publication" >&2
    exit 1
}

bash "$script_dir/check-release-blockers.sh"
gh release edit "$final_tag" --repo "$GITHUB_REPOSITORY" --draft=false

for attempt in $(seq 1 12); do
    release_body=$(mktemp)
    gh api -H 'X-GitHub-Api-Version: 2026-03-10' \
        "repos/$GITHUB_REPOSITORY/releases/$release_id" >"$release_body"
    if jq -e '.draft == false and .immutable == true' "$release_body" >/dev/null; then
        remote_final_commit=$(git ls-remote origin "refs/tags/$final_tag^{}" | awk '{ print $1 }')
        [[ "$remote_final_commit" == "$commit_sha" ]] || {
            echo "immutable release tag does not point to the manifest commit" >&2
            exit 1
        }
        immutable_names=$(jq -r '.assets[].name' "$release_body" | LC_ALL=C sort)
        [[ "$immutable_names" == "$expected_names" ]] || {
            echo "immutable release assets differ from the canonical asset set" >&2
            exit 1
        }
        verify_dir=$(mktemp -d)
        gh release download "$final_tag" --repo "$GITHUB_REPOSITORY" --dir "$verify_dir"
        for asset in "${assets[@]}"; do
            source_file="$candidate_dir/$asset"
            [[ "$asset" == test-evidence.json ]] && source_file=$evidence_file
            [[ "$asset" == *.asc ]] && source_file="$release_assets/$asset"
            [[ -f "$verify_dir/$asset" ]] && cmp -s "$source_file" "$verify_dir/$asset" || {
                echo "immutable release asset $asset is missing or differs" >&2
                exit 1
            }
        done
        jq -r '.html_url' "$release_body"
        exit 0
    fi
    echo "waiting for immutable release state ($attempt/12)"
    sleep 5
done

echo "GitHub Release was published but did not report immutable state" >&2
exit 1
