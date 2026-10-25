#!/usr/bin/env python3
"""Prepare a reproducible candidate commit without release credentials."""

from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys
import tomllib

import signing

REPOSITORY_URL = "https://github.com/scylladb/alternator-client-rust"
VERSION = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"


def bump_files(root, version, date):
    if not re.fullmatch(VERSION, version):
        raise ValueError("Version must be an exact X.Y.Z release without leading zeroes.")
    manifest = root / "Cargo.toml"
    current = tomllib.loads(manifest.read_text())["package"]["version"]
    if tuple(map(int, version.split("."))) < tuple(map(int, current.split("."))):
        raise ValueError("Release version cannot decrease.")
    lockfile = root / "Cargo.lock"
    lock = tomllib.loads(lockfile.read_text())
    entries = [p for p in lock["package"] if p["name"] == "alternator-client"]
    if len(entries) != 1 or entries[0]["version"] != current:
        raise ValueError("Cargo.lock does not match the package version.")

    changelog = root / "CHANGELOG.md"
    text = changelog.read_text()
    unreleased = re.search(r"(?m)^## \[Unreleased\]\n(.*?)(?=^## |^\[Unreleased\]:|\Z)", text, re.S)
    if not unreleased:
        raise ValueError("CHANGELOG.md needs an Unreleased section.")
    notes = unreleased[1].strip()
    existing = re.search(r"(?m)^## \[" + re.escape(version) + r"\] - \d{4}-\d{2}-\d{2}$", text)
    if existing and version != current:
        raise ValueError("Release version already has a changelog entry.")
    if existing:
        text = text[:existing.start()] + f"## [{version}] - {date}" + text[existing.end():]
        text = text[:unreleased.start()] + "## [Unreleased]\n\n" + text[unreleased.end():]
        if notes:
            text = text.replace(f"## [{version}] - {date}\n", f"## [{version}] - {date}\n\n{notes}\n", 1)
    else:
        if not notes:
            raise ValueError("Add release notes under Unreleased before dispatching a new version.")
        text = (text[:unreleased.start()] + f"## [Unreleased]\n\n## [{version}] - {date}\n\n{notes}\n\n"
                + text[unreleased.end():])
    text, count = re.subn(r"(?m)^\[Unreleased\]: .*$",
                          f"[Unreleased]: {REPOSITORY_URL}/compare/v{version}...HEAD", text)
    if count != 1:
        raise ValueError("CHANGELOG.md needs exactly one Unreleased comparison link.")
    if not re.search(r"(?m)^\[" + re.escape(version) + r"\]:", text):
        link = f"[{version}]: {REPOSITORY_URL}/compare/v{current}...v{version}"
        text = text.replace(f"[Unreleased]: {REPOSITORY_URL}/compare/v{version}...HEAD\n",
                            f"[Unreleased]: {REPOSITORY_URL}/compare/v{version}...HEAD\n{link}\n")

    updated_manifest, count = re.subn(r'(?m)^(\[package\]\n(?:(?!\[).)*?^version = ")[^"]+("$)',
                                    lambda m: m[1] + version + m[2], manifest.read_text(), count=1, flags=re.S)
    if count != 1:
        raise ValueError("Could not locate the package version in Cargo.toml.")
    updated_lock, count = re.subn(r'(?m)^(\[\[package\]\]\nname = "alternator-client"\nversion = ")[^"]+("$)',
                                lambda m: m[1] + version + m[2], lockfile.read_text())
    if count != 1:
        raise ValueError("Could not locate the package version in Cargo.lock.")
    manifest.write_text(updated_manifest)
    lockfile.write_text(updated_lock)
    changelog.write_text(text)


def prepare(version, expected_commit=None):
    if not re.fullmatch(VERSION, version):
        raise ValueError("Version must be an exact X.Y.Z release without leading zeroes.")
    source = os.environ["GITHUB_SHA"]
    run_id = os.environ["GITHUB_RUN_ID"]
    if os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("Release workflow must be dispatched from main.")
    signing.run(["git", "fetch", "--force", "--tags", "origin",
                 "+refs/heads/main:refs/remotes/origin/main"])
    owned = []
    for tag in signing.run(["git", "tag", "-l", f"v{version}-rc.*"]).splitlines():
        if f"workflow-run: {run_id}" in signing.run(["git", "cat-file", "-p", tag]).splitlines():
            owned.append(tag)
    if len(owned) > 1:
        raise ValueError("Multiple RC tags belong to this workflow run.")
    if owned:
        signing.verify_tag(owned[0])
        commit = signing.run(["git", "rev-parse", owned[0] + "^{commit}"]).strip()
        if signing.run(["git", "show", "-s", "--format=%P", commit]).strip() != source:
            raise ValueError("Recovered release commit is not a child of the dispatch commit.")
        if expected_commit and commit != expected_commit:
            raise ValueError("Recovered release commit differs from the verified candidate.")
        signing.run(["git", "checkout", "--detach", commit])
        return
    if (signing.run(["git", "rev-parse", "HEAD"]).strip() != source
            or (not expected_commit
                and signing.run(["git", "rev-parse", "origin/main"]).strip() != source)
            or signing.run(["git", "status", "--porcelain"]).strip()):
        raise ValueError("Release preparation requires a clean checkout of current main.")
    # The workflow creation time is fixed across jobs and reruns. Signing this
    # commit later would change its SHA and Cargo's packaged VCS metadata;
    # signed tags authenticate the unchanged, tested commit after approval.
    timestamp = datetime.fromisoformat(os.environ["RELEASE_TIMESTAMP"])
    if timestamp.tzinfo is None:
        raise ValueError("Release timestamp must include a timezone.")
    timestamp = timestamp.astimezone(timezone.utc)
    bump_files(Path.cwd(), version, timestamp.date().isoformat())
    signing.run(["git", "add", "Cargo.toml", "Cargo.lock", "CHANGELOG.md"])
    commit_env = {**os.environ,
                  "GIT_AUTHOR_NAME": "github-actions[bot]",
                  "GIT_AUTHOR_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
                  "GIT_COMMITTER_NAME": "github-actions[bot]",
                  "GIT_COMMITTER_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
                  "GIT_AUTHOR_DATE": timestamp.isoformat(),
                  "GIT_COMMITTER_DATE": timestamp.isoformat()}
    signing.run(["git", "-c", "core.hooksPath=/dev/null", "commit", "--no-gpg-sign",
                 "--allow-empty", "--cleanup=verbatim", "-m",
                 f"chore: release {version}\n\nworkflow-run: {run_id}\nsource-commit: {source}\n"],
                env=commit_env)
    if expected_commit and signing.run(["git", "rev-parse", "HEAD"]).strip() != expected_commit:
        raise ValueError("Recreated release commit differs from the verified candidate.")


if __name__ == "__main__":
    try:
        if len(sys.argv) not in (2, 3):
            raise ValueError("Usage: prepare_release.py VERSION [EXPECTED_COMMIT]")
        prepare(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else None)
    except (ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
