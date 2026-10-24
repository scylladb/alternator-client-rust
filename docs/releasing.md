# Releasing alternator-client

The package name is `alternator-client`; the Rust library name remains
`alternator_driver`. Dispatch `.github/workflows/release.yml` from `main` with
an exact final version such as `1.1.0`. The pipeline bumps `Cargo.toml`,
`Cargo.lock`, and `CHANGELOG.md`, creates a signed release commit and signed
annotated tags, verifies the packaged candidate, and publishes an immutable
GitHub Release containing the exact tested crate.

Never move or delete a release tag. Never publish from a development checkout
or create release commits, tags, or GitHub Releases manually.

## Repository administration

Complete these settings before dispatching a release:

1. In **Settings > General > Releases**, enable **release immutability**.
   Existing mutable releases are unaffected. The workflow requires a published
   release to report `immutable: true` before succeeding. Its token has no
   Administration access, so keep this repository setting enabled: disabling it
   can allow publication before the final check fails.
2. Keep an active tag ruleset matching `v*`, with **Restrict updates** and
   **Restrict deletions**, no creation restriction, and no bypass actors. This
   protects both RC and final tags from their creation, including before the
   final GitHub Release is published.
3. Create an environment named **`release`**. Set **Required reviewers** to the
   **`scylladb/drivers`** team. Any one member can approve. Leave **Prevent
   self-review** disabled so a team member who dispatches a release can approve
   it. Disable administrator bypass. Under **Selected branches and tags**, allow
   only the `main` branch, with no tag patterns.
4. Store `GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` **only in `release`**. The key
   must be the protected, repository-specific signing-subkey export described
   in [SIGNING.md](../scripts/release/SIGNING.md), registered on
   `scylladb-publisher`. Never upload the primary private key. Delete duplicate
   repository secrets after verifying the environment copies; ensure no
   organization secret exposes these credentials to this repository.
5. For the first publication only, store a short-lived crates.io token in
   **`release`** as `CRATES_IO_BOOTSTRAP_TOKEN`. Limit it to `publish-new` for
   `alternator-client`. Revoke and delete it immediately after bootstrap.
   Subsequent releases use OIDC; no persistent crates.io token is needed.

The former `crates-io` environment is unused. If a trusted publisher already
exists, change its environment binding to `release` before the next dispatch.
Environment protection is configured in GitHub settings, not in workflow YAML;
a newly named environment has no reviewers until an administrator configures it.

References: [immutable releases](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/establish-provenance-and-integrity/prevent-release-changes),
[tag rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets),
and [deployment environments](https://docs.github.com/en/actions/reference/workflows-and-actions/deployments-and-environments).

## Prepare notes and dispatch

Merge the release content through normal reviewed PRs. Add notes under
`## [Unreleased]` in `CHANGELOG.md`; do not manually bump the version. For a new
version, an empty Unreleased section is rejected. The existing `1.0.0` section
can be used for the initial publication, with its date updated by the pipeline.
Release versions cannot decrease.

Dispatch from Actions, or run:

```sh
gh workflow run release.yml --ref main -f version=1.1.0
```

Dispatch starts verification. It does not approve release credentials.

## Verification and approvals

The pipeline has two approval points, both after verification:

1. **Source verification:** the reusable ordinary CI workflow runs static,
   security, packaging, portable, and Scylla tests on the dispatch commit, with
   no deployment environment or release secrets.
2. **Preparation approval:** after those jobs succeed, the `release` environment
   waits for one member of `scylladb/drivers`. Approval unlocks the signing key.
   The pipeline bumps both Cargo versions, promotes Unreleased notes to a dated
   version section, updates comparison links, and creates a signed release
   commit as a direct child of the dispatch commit. It creates the immutable
   signed `vX.Y.Z-rc.N` tag at that commit. It does not push to `main`.
3. **Exact candidate verification:** ordinary runners package and attest that
   signed commit before testing. Portable, static, security, compatibility,
   publication dry-run, and Scylla gates extract and test the same candidate.
   These jobs have no deployment environment or release credentials.
4. **Publication approval:** all candidate gates and test evidence must succeed
   before GitHub requests a second approval from `scylladb/drivers`. This is a
   separate deployment to `release`. Failed or skipped gates block publication.
5. **Release:** the approved job verifies candidate identity and regenerated
   crate bytes, obtains the registry credential, publishes, and requires both
   the crates.io index checksum and downloaded archive to match the candidate.
   It then creates signed `vX.Y.Z` at the same signed RC commit, uploads and
   verifies release assets in a draft, and publishes the immutable GitHub Release.

All pipeline-created release commits, tags, and detached asset signatures use
the same repository-specific GPG subkey belonging to `scylladb-publisher`.
GitHub Releases have no GPG-signature field: the signed tag and signed manifest,
crate, checksums, SBOM, and evidence authenticate their contents. GitHub adds
its own release attestation for immutable releases.

The source checkout must still be current `main` when a new release commit is
prepared. The version bump lives on the tagged release commit. Carry that
commit's version and historical changelog entries back to `main` through a
normal reviewed PR before preparing the next release's Unreleased notes.

## Candidate and evidence

The immutable Actions candidate artifact is named
`alternator-client-vX.Y.Z-rc.N-attempt-K` and contains exactly:

- `alternator-client-X.Y.Z.crate`;
- `SHA256SUMS`;
- `alternator-client-X.Y.Z.cdx.json`;
- `release-manifest.json`.

The manifest records the signed release commit, crate and SBOM hashes, pinned
inputs, lockfile and changelog hashes. Build provenance attests these files;
its workflow source digest is the dispatch commit. The signed manifest binds
the generated release commit to the tested package. The SBOM attestation
attests the crate itself with the exact CycloneDX predicate.

`release-evidence-vX.Y.Z-rc.N-attempt-K` contains `test-evidence.json`. Both
artifacts are retained for 90 days. Final release assets include these five
files and a detached `.asc` signature for each.

Scylla 2025.1 predates Alternator HTTP compression, so its server gate excludes
the five compression interoperability cases. They run against 2026.1;
portable compression coverage runs on every target. Both server versions run
the complete CCM routing and load-balancing suite.

## Initial publication and Trusted Publishing

crates.io requires a crate to exist before a trusted publisher can be added.
The approved release job therefore publishes an unclaimed `alternator-client`
`1.0.0` using `CRATES_IO_BOOTSTRAP_TOKEN` from the `release` environment. A
missing token stops publication; the workflow never asks for local publishing.
An existing claimed name without `1.0.0` is rejected by preflight.

After the exact `1.0.0` bytes are visible, revoke and delete the bootstrap token.
On crates.io, add a trusted publisher with:

- owner: `scylladb`;
- repository: `alternator-client-rust`;
- workflow filename: `release.yml`;
- environment: `release`.

Enable **Require trusted publishing for all new versions** after saving it.
Keep the environment's `main` restriction: trusted publishing does not itself
restrict the branch. See [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).

## Failures and recovery

An open issue labeled exactly `release-blocker` stops release mutations. The
check verifies that the label exists and fails closed on API, authentication,
rate-limit, pagination, or malformed-response errors. Pull requests with the
label do not count. Checks run before creating an RC tag, publishing to
crates.io, creating the final tag, and publishing the draft GitHub Release.

For infrastructure failures or interrupted publication, rerun failed jobs in
the same run:

```sh
gh run rerun RUN_ID --repo scylladb/alternator-client-rust --failed
```

Preparation recovers the existing run-owned signed RC commit, verifies its
signature and parent, and does not repeat the version bump. Regenerated crate
bytes must match earlier artifacts or the preserved candidate hash in the
previous attempt's log. Recovery requires the relevant environment approval
again. Existing exact registry bytes skip publication. Existing tags and draft
assets are verified and reused; signatures are never overwritten.

For source failures, merge a normal fix PR and dispatch a new run for the same
version. It creates a new RC number. Older RCs cannot be newly published after
a newer RC exists. Never replace tags, assets, or a published crate version.
A conflicting registry checksum or release asset stops recovery.

Pinned Rust, nightly, CCM, Scylla, release-tool, runner, and action revisions
change only through reviewed PRs. Release matrices live in
`scripts/release/release-policy.json`. Weekly maintenance validates moving
inputs without publishing or committing updates.
