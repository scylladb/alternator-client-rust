# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `CCAE6AC1D761E6243D35A8E6D5C833581E9E6D22`
* Signing fingerprint: `966F8EB73EE80D52D3DBE4484B354DBA3B45955B`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-rust) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:24+00:00`

The protected `release` environment secrets `GPG_PRIVATE_KEY` (ASCII-armored protected signing-subkey
export) and `GPG_PASSPHRASE` (single-line passphrase) supply release signing.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

`signing.py configure` checks that the imported private key is the protected signing
subkey without usable primary private material. It performs a fresh-agent signing
preflight and verifies the exact signing and certification fingerprints against a
separate public-only keyring. Anonymous pipes supply the key import and passphrase; neither credential is
written to a temporary file or included in command-line arguments. Each signing
step receives its secret explicitly; setup does not write it into `GITHUB_ENV`. Git configuration is job-specific
and inherited by release-tool clones. Always run `signing.py cleanup` after a CI job.

The release workflow prepares and tests a reproducible unsigned version/changelog
commit without signing credentials. After its single `scylladb/drivers` approval,
it receives signing secrets and creates signed RC and final tags at that exact
commit, plus detached signatures for the release assets. Signing the commit
itself at that point would change the SHA embedded in the tested crate.
Do not duplicate these secrets at repository or organization scope.

For offline signing validation, use an isolated invocation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
python3 scripts/release/signing.py exec true
```

The `exec` command restores checkout signing configuration on exit. Do not create
release commits or tags locally, or change existing release tags. A failed
verification blocks publication.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys CCAE6AC1D761E6243D35A8E6D5C833581E9E6D22
gpg --fingerprint CCAE6AC1D761E6243D35A8E6D5C833581E9E6D22
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require exactly one `GOODSIG` and one `VALIDSIG` record with the signing fingerprint
above and the expected primary fingerprint. Reject expired, revoked, bad, or failed
signature status records. `git verify-tag --raw TAG` and `git verify-commit --raw COMMIT`
provide the equivalent check for Git objects. Keyserver presence alone establishes
neither trust nor the expected repository identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.
