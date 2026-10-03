# Releasing alternator-client

This runbook covers releases of the `alternator-client` package. The Rust
library name remains `alternator_driver`, so downstream Rust code continues to
import it with `use alternator_driver::...`.

The release decision is a manual dispatch of `.github/workflows/release.yml`
from `main`. That workflow creates an immutable release-candidate tag, packages
the final version, and tests that exact package. A successful candidate is
published automatically, except for the one-time local publication of the
initial `1.0.0` version described below.

Never move or delete an RC tag. Never publish from a working copy of `main`.
Never create an RC GitHub Release. The first GitHub Release for a version is the
final release created after crates.io contains the tested bytes.

## One-time repository administration

Complete these steps before dispatching the first candidate.

### Enable immutable GitHub Releases

In **Settings > General > Releases**, select **Enable release immutability**.
This setting applies only to releases published after it is enabled. Immutable
releases lock their assets and associated tag and receive a GitHub release
attestation. Titles and notes remain editable, so the attached manifest and
changelog hash are the canonical release metadata.

Treat this as a permanent repository-administration invariant. The release
workflow deliberately does not hold an Administration token and therefore
cannot check the setting before publication. It does require the published
release to report `immutable: true` before declaring success. If an
administrator disables immutability, finalization may publish a mutable
release before that verification fails; re-enable the setting before any
release dispatch rather than relying on recovery afterward.

References:

- [Enable immutable releases](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/prevent-release-changes)
- [What immutable releases protect](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)

### Protect RC tags

In **Settings > Rules > Rulesets**, create an active tag ruleset with:

- target pattern `v*` (covering both RC and final release tags);
- **Restrict updates** enabled;
- **Restrict deletions** enabled;
- **Restrict creations** disabled; and
- no bypass actors.

This lets the workflow create `vX.Y.Z-rc.N` and `vX.Y.Z` once but prevents
anyone from moving or deleting either tag. Protecting final tags immediately
also closes the short interval between final-tag creation and publication of
the immutable GitHub Release; release immutability adds a second lock afterward.

See [GitHub's ruleset rule definitions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets).

### Create the crates.io environment

In **Settings > Environments**, create an environment named `crates-io`.
Under deployment branches and tags, choose **Selected branches and tags**, add
the branch `main`, and add no tag patterns. Do not add required reviewers:
passing candidates are deliberately promoted without a second approval. Disable
administrator bypass if the repository plan offers that option.

Do not add an Administration or crates.io token to this environment. In
particular, there is no bootstrap token or `CARGO_REGISTRY_TOKEN` GitHub secret.
The first publish is a local operation; subsequent publishes use short-lived
OIDC credentials.

The publishing job in `release.yml` must declare `environment: crates-io`.
The environment restriction is an independent guard in addition to the
workflow's check that the dispatch commit is the current `main` commit.

See [GitHub deployment environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments#deployment-branches-and-tags).

## Prepare a release PR

Prepare and merge a normal reviewed PR that:

1. sets the package version in `Cargo.toml` to the final `X.Y.Z`;
2. refreshes `Cargo.lock` so its `alternator-client` entry has the same version;
3. adds a dated `X.Y.Z` section to `CHANGELOG.md`; and
4. updates the `[Unreleased]` comparison link.

Use the final version in the package. Do not put `-rc.N` in Cargo metadata; only
the Git tag contains the RC suffix. For the initial release these values are
`alternator-client` and `1.0.0`.

Before merging, let ordinary PR CI finish. The release workflow repeats the
release gates against the packaged candidate rather than trusting the checkout.

## Dispatch and observe a candidate

From the Actions page, select **Release**, choose **Run workflow**, select
`main`, and enter the exact version from `Cargo.toml`. Equivalently:

```sh
gh workflow run release.yml --ref main -f version=1.0.0
```

Dispatch is the release approval. After it starts, promotion is automatic when
all gates pass; do not add an environment reviewer or a separate approval step.

The workflow rejects a non-`main` dispatch, a version or changelog mismatch, an
existing final tag, or a version that was already published outside recovery.
It selects the next unused RC number and atomically creates an annotated tag,
for example `v1.0.0-rc.1`.

An open GitHub issue with the exact `release-blocker` label stops release
mutation; pull requests with that label do not count. The check verifies that
the label exists, queries every page of matching open issues, and fails closed
on missing label configuration, API, authentication, or rate-limit errors,
pagination failures, and malformed responses.

The release process checks immediately before creating a new RC tag,
publishing an absent version to crates.io, pushing a missing final tag, and
publishing the verified draft GitHub Release. Existing exact registry bytes,
run-owned tags, draft releases, and published immutable releases remain
authoritative recovery state. After a blocker is cleared or API access
recovers, rerun the failed jobs from the same workflow run; there is no blocker
bypass.

The candidate artifact is named
`alternator-client-vX.Y.Z-rc.N-attempt-K`, where `K` is the workflow attempt.
It contains:

- `alternator-client-X.Y.Z.crate`;
- `SHA256SUMS`;
- the CycloneDX JSON SBOM; and
- `release-manifest.json`.

The package is built and attested before testing. Every candidate test extracts
and tests that package. Test evidence is uploaded separately. If any gate fails,
there is no crates.io publish, final tag, or GitHub Release.

Scylla 2025.1 predates Alternator HTTP request and response compression. Its
server gate therefore excludes the five compression interoperability cases;
those cases run against 2026.1, while portable client-side compression coverage
still runs on every target. Both release lines run the complete CCM routing and
load-balancing suite.

The evidence artifact is named
`release-evidence-vX.Y.Z-rc.N-attempt-K`, is retained for 90 days, and contains
`test-evidence.json` with the expected matrix targets and recorded job results.

For a code or packaging failure, merge a normal fix PR and dispatch the same
version again. The old tag, artifact, attestation, evidence, and logs remain;
the new commit receives `rc.N+1`. Do not rerun an old RC after changing source.

For an infrastructure flake, rerun the same workflow run/RC. GitHub assigns the
rerun a higher attempt number. Its regenerated crate must have the same SHA-256
as the prior candidate for that RC. A full rerun replaces the prior Actions
artifact, so the packaging job also preserves the candidate digest in the
prior attempt's log and checks the regenerated crate against that record.

## Bootstrap `alternator-client` 1.0.0 locally

crates.io cannot configure a trusted publisher until the crate exists. The
first passing `1.0.0` workflow therefore stops intentionally at its publication
job when it confirms that `alternator-client` is unclaimed. This is the expected
bootstrap state, not a failed candidate. Keep that workflow run ID: after the
local publish, the same run will be resumed so it can verify and finalize the
same RC.

Cargo cannot upload a prebuilt `.crate`; `cargo publish` always packages the
source again. To preserve the artifact-first guarantee, perform the local
publish from a fresh checkout of the immutable RC tag, with Rust/Cargo `1.94.1`,
and prove that the regenerated archive is byte-for-byte identical to the tested
archive before publishing. Do not extract the candidate and run Cargo inside
the archive: Cargo package archives contain generated reserved files and are
not publication source trees. Do not use an existing workspace checkout, even
if it currently appears clean.

Prefer an Ubuntu 24.04 x86-64 machine, matching the packaging host. The byte
comparison remains mandatory and safely stops publication if host differences
change the archive.

Run all commands below in the same dedicated Bash session; they deliberately
use a new temporary directory and stop on failed checks. Set `RUN_ID`, `RC_TAG`,
and `ARTIFACT_NAME` from the stopped workflow. Use an authenticated `gh`
session with read access to the repository and its issues.

```sh
set -euo pipefail
export RUN_ID=123456789
export RC_TAG=v1.0.0-rc.1
export ARTIFACT_NAME=alternator-client-v1.0.0-rc.1-attempt-1
export REPOSITORY=scylladb/alternator-client-rust
export GITHUB_REPOSITORY="$REPOSITORY"
export GH_TOKEN="$(gh auth token)"

release_root="$(mktemp -d)"
git clone --no-checkout https://github.com/scylladb/alternator-client-rust.git \
  "$release_root/source"
git -C "$release_root/source" fetch --force origin \
  "refs/tags/$RC_TAG:refs/tags/$RC_TAG"
git -C "$release_root/source" checkout --detach "$RC_TAG^{commit}"
test -z "$(git -C "$release_root/source" status --porcelain)"
test "$(git -C "$release_root/source" cat-file -t "$RC_TAG")" = tag
test "$(git -C "$release_root/source" rev-parse "$RC_TAG^{commit}")" = \
  "$(git -C "$release_root/source" rev-parse HEAD)"

mkdir "$release_root/candidate"
gh run download "$RUN_ID" --repo "$REPOSITORY" \
  --name "$ARTIFACT_NAME" --dir "$release_root/candidate"
```

Verify the candidate's checksums, manifest identity, build-provenance
attestation, and exact CycloneDX SBOM attestation. The workflow attests the
`.crate` itself, so point `gh attestation` at that file rather than at the
Actions artifact ZIP.

```sh
RC_COMMIT="$(git -C "$release_root/source" rev-parse HEAD)"
mkdir "$release_root/extracted"
bash "$release_root/source/scripts/release/verify-candidate.sh" \
  "$release_root/candidate" "$release_root/extracted" \
  1.0.0 "$RC_TAG" "$RC_COMMIT"

gh attestation verify \
  "$release_root/candidate/alternator-client-1.0.0.crate" \
  --repo "$REPOSITORY" \
  --signer-workflow "$REPOSITORY/.github/workflows/release.yml" \
  --source-digest "$RC_COMMIT"

sbom_attestation="$(mktemp)"
gh attestation verify \
  "$release_root/candidate/alternator-client-1.0.0.crate" \
  --repo "$REPOSITORY" \
  --signer-workflow "$REPOSITORY/.github/workflows/release.yml" \
  --source-digest "$RC_COMMIT" \
  --predicate-type https://cyclonedx.org/bom \
  --format json >"$sbom_attestation"

jq -e --slurpfile sbom \
  "$release_root/candidate/alternator-client-1.0.0.cdx.json" \
  'any(.[]; .verificationResult.statement.predicate == $sbom[0])' \
  "$sbom_attestation" >/dev/null
```

Install and select the exact release toolchain, regenerate the archive, and
compare both its bytes and digest with the candidate. Run the repository's
package inspection too.

```sh
rustup toolchain install 1.94.1 --profile minimal --no-self-update
local_package_target="$(mktemp -d)"
(
  cd "$release_root/source"
  cargo +1.94.1 package --locked --target-dir "$local_package_target"
  bash scripts/release/inspect-package.sh 1.0.0 \
    "$local_package_target/package/alternator-client-1.0.0.crate"
)

cmp \
  "$release_root/candidate/alternator-client-1.0.0.crate" \
  "$local_package_target/package/alternator-client-1.0.0.crate"

test "$(shasum -a 256 \
  "$local_package_target/package/alternator-client-1.0.0.crate" | awk '{print $1}')" = \
  "$(jq -r '.crate.sha256' \
  "$release_root/candidate/release-manifest.json")"
test -z "$(git -C "$release_root/source" status --porcelain)"
```

If any command above fails, stop. Do not publish and do not weaken or bypass a
check. Resolve the discrepancy through a normal PR and a new RC.

Immediately before obtaining a crates.io token, confirm that no newer `1.0.0`
candidate has superseded this RC:

```sh
bash "$release_root/source/scripts/release/assert-latest-rc.sh" \
  1.0.0 "$RC_TAG" "$RC_COMMIT"
```

If this fails, do not publish the older candidate. Fix or validate the newest
RC instead.

Using the crates.io account that will own the new package, with its email
address verified, create a local API token with endpoint scope `publish-new`,
crate scope exactly `alternator-client`, and the shortest practical expiry.
Copy it into an unexported shell variable without echoing or committing it.
Recheck release blockers immediately before publication, then expose the token
only to Cargo and publish from the same unchanged RC checkout:

```sh
read -rsp 'crates.io token: ' registry_token
printf '\n'
bash "$release_root/source/scripts/release/check-release-blockers.sh"
export CARGO_REGISTRY_TOKEN="$registry_token"
unset registry_token
publish_status=0
(
  cd "$release_root/source"
  publish_target="$(mktemp -d)"
  cargo +1.94.1 publish --locked --no-verify --registry crates-io \
    --target-dir "$publish_target"
) || publish_status=$?
unset CARGO_REGISTRY_TOKEN
printf 'cargo publish exit status: %s\n' "$publish_status"
```

Revoke the token on crates.io immediately after the command, including when the
command reports a timeout or another ambiguous error. Do not retry until the
registry checks below establish whether the upload succeeded.

Poll until the `1.0.0` sparse-index record appears, then require its checksum
and the downloaded registry archive to equal the tested candidate. The
compressed archive SHA-256 is the sparse index `cksum` value.

```sh
candidate_sha="$(jq -r '.crate.sha256' \
  "$release_root/candidate/release-manifest.json")"

index_record=
for attempt in {1..60}; do
  if index_record="$(curl --fail --silent --show-error \
    --user-agent alternator-client-release-bootstrap \
    https://index.crates.io/al/te/alternator-client | \
    jq -cer 'select(.vers == "1.0.0")')"; then
    break
  fi
  sleep 10
done
test -n "$index_record"

test "$(jq -r '.cksum' <<<"$index_record")" = "$candidate_sha"
registry_crate="$release_root/registry-alternator-client-1.0.0.crate"
for attempt in {1..60}; do
  if curl --fail --location --silent --show-error --max-time 20 \
    --user-agent alternator-client-release-bootstrap \
    https://static.crates.io/crates/alternator-client/alternator-client-1.0.0.crate \
    --output "$registry_crate.partial"; then
    mv "$registry_crate.partial" "$registry_crate"
    break
  fi
  sleep 10
done
test -f "$registry_crate"
test "$(shasum -a 256 \
  "$registry_crate" | awk '{print $1}')" = \
  "$candidate_sha"
cmp \
  "$release_root/candidate/alternator-client-1.0.0.crate" \
  "$registry_crate"
```

A different checksum is an unrecoverable security conflict: do not create the
final tag or release, and escalate immediately. If the version remains absent,
investigate the local publish before creating a new token. Never try to replace
or republish version `1.0.0`.

## Configure Trusted Publishing and finish `1.0.0`

Once the verified `alternator-client` `1.0.0` archive is visible on crates.io,
open the crate's **Settings > Trusted Publishing** page and add a GitHub
publisher with these exact values:

- GitHub owner: `scylladb`
- repository: `alternator-client-rust`
- workflow filename: `release.yml`
- environment: `crates-io`

Enter only the workflow filename, not `.github/workflows/release.yml`. Enable
**Require trusted publishing for all new versions** after the publisher is
saved. Trusted Publishing matches the repository, workflow filename, and
environment, but not the branch; keep the `main` environment restriction and
the workflow's default-branch checks.

See [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).

Resume only the failed jobs in the original bootstrap run:

```sh
gh run rerun "$RUN_ID" --repo "$REPOSITORY" --failed
```

This reruns the failed jobs and their dependent jobs; successful packaging and
test jobs remain tied to the original run and RC.

Do not start a fresh dispatch: preflight correctly rejects an already-published
version. On rerun, publication recovery must find the existing sparse-index
record, require its checksum and downloaded archive to match the candidate,
skip uploading, and continue. Finalization then creates annotated tag `v1.0.0`
at the RC commit, creates a draft GitHub Release, attaches the candidate crate,
checksums, SBOM, manifest, and evidence, and publishes the draft. Confirm the
release is non-draft and is shown as **Immutable** before declaring completion.

## Subsequent releases

After bootstrap, dispatch `release.yml` from `main` with the prepared `X.Y.Z`.
The publishing job obtains a short-lived credential through crates.io Trusted
Publishing; no persistent crates.io credential is stored in GitHub. It
regenerates the crate from the immutable RC tag and proves byte equality with
the tested artifact before `cargo publish --locked`.

After Cargo returns, or after any ambiguous publish failure, the job polls the
sparse index and downloads the registry archive. It continues only if both
hashes equal the candidate. Verified registry presence defines “released.” Only
then may finalization create `vX.Y.Z` and the immutable GitHub Release.

## Maintaining pinned inputs

`.github/workflows/maintenance.yml` runs weekly without publishing anything.
One job resolves the newest compatible dependency graph and runs the shared
source/portable gates; the other tests the moving `2026.1` and `2025.1` Scylla
release lines. Treat its failures as update signals, not as permission for a
workflow to rewrite release inputs.

Change Rust, dated nightly, CCM, exact Scylla patch, release-tool, runner, or
action-SHA pins only in a normal reviewed PR. Validate the replacement pins in
CI before merging. The portable and pinned-Scylla release matrices have a
single reviewed source in `scripts/release/release-policy.json`; the workflow,
candidate manifest, verifier, and evidence record all read it. The policy also
records which pinned server release supports Alternator HTTP compression. Never
make a scheduled job commit those updates directly.

## Failure and recovery reference

- **Preflight, package, or test failure:** preserve the RC and all evidence;
  merge a fix and dispatch the same final version to obtain `rc.N+1`.
- **Infrastructure failure before publish:** rerun the same RC. A regenerated
  package with a different digest is a hard failure.
- **Ambiguous publish result:** query the sparse index and registry archive. If
  absent, retry the same RC; if the digest matches, resume finalization; if it
  differs, stop and escalate.
- **crates.io succeeded but finalization failed:** rerun the failed jobs from
  the same workflow run. Recovery verifies the existing registry bytes before
  creating or completing the final tag and GitHub Release.
- **Final tag exists without a published immutable release:** do not move it.
  Investigate and resume the same finalization job; do not dispatch another RC.
- **Published GitHub Release exists:** never update/delete assets or automate
  release/tag deletion. Notes may be corrected, but the attached manifest and
  changelog hash remain canonical.

At completion, the crates.io archive, tested Actions artifact, manifest digest,
and GitHub Release asset must all identify the same bytes and commit.
