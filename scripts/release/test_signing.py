"""Exercise release signing with disposable protected keys and real Git objects."""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import signing
import prepare_release


@unittest.skipUnless(shutil.which("gpg") and shutil.which("git"), "GnuPG and Git required")
class SigningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory(prefix="repo-signing-fixture-")
        cls.directory = Path(cls.fixture.name)
        cls.keyring = cls.directory / "gnupg"
        cls.keyring.mkdir(mode=0o700)
        cls.password = "test-only-release-passphrase"
        def gpg(*args):
            result = subprocess.run(
                ["gpg", "--no-options", "--homedir", str(cls.keyring), "--batch",
                 "--pinentry-mode", "loopback", "--passphrase-fd", "0", *args],
                input=cls.password + "\n", capture_output=True, text=True, check=True,
            )
            return result.stdout
        gpg("--quick-generate-key", "Release Test <release@example.invalid>", "rsa2048", "cert", "1d")
        cls.primary = [line.split(":")[9] for line in
                       gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][0]
        gpg("--quick-add-key", cls.primary, "rsa2048", "sign", "1d")
        cls.subkey = [line.split(":")[9] for line in
                      gpg("--with-colons", "--list-keys").splitlines() if line.startswith("fpr:")][1]
        cls.public = cls.directory / "public.asc"
        cls.public.write_text(gpg("--armor", "--export", cls.primary))
        cls.private = cls.directory / "subkey.asc"
        cls.private.write_text(gpg("--armor", "--export-secret-subkeys", cls.primary))
        cls.full = cls.directory / "full.asc"
        cls.full.write_text(gpg("--armor", "--export-secret-keys", cls.primary))

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["gpgconf", "--homedir", str(cls.keyring), "--kill", "gpg-agent"],
                       capture_output=True, check=True)
        cls.fixture.cleanup()

    @contextmanager
    def repository(self, full=False, password=None):
        original = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="repo-signing-test-") as temp:
            directory = Path(temp)
            keyring = directory / "gnupg"
            keyring.mkdir(mode=0o700)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "RELEASE_GPG_"))
                   and key not in {"MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE"}}
            env.update(GNUPGHOME=str(keyring), RUNNER_TEMP=str(directory),
                       GITHUB_ENV=str(directory / "github-env"), GIT_CONFIG_NOSYSTEM="1",
                       GIT_CONFIG_GLOBAL=str(directory / "initial-gitconfig"),
                       MAVEN_GPG_PASSPHRASE=self.password if password is None else password)
            (directory / "initial-gitconfig").write_text("")
            subprocess.run(["gpg", "--no-options", "--homedir", str(keyring), "--batch",
                            "--pinentry-mode", "loopback", "--passphrase-fd", "0", "--import",
                            str(self.full if full else self.private)], input=self.password + "\n",
                           capture_output=True, text=True, env=env, check=True)
            try:
                with patch.dict(os.environ, env, clear=True), \
                        patch.multiple(signing, PRIMARY=self.primary, SIGNING=self.subkey,
                                       PUBLIC_KEY=self.public):
                    repo = directory / "source"
                    signing.run(["git", "init", "--quiet", str(repo)])
                    os.chdir(repo)
                    signing.run(["git", "-c", "user.name=Initial", "-c",
                                 "user.email=initial@example.invalid", "-c", "commit.gpgsign=false",
                                 "commit", "--quiet", "--allow-empty", "-m", "Initial"])
                    yield directory
            finally:
                os.chdir(original)
                subprocess.run(["gpgconf", "--homedir", str(keyring), "--kill", "gpg-agent"],
                               capture_output=True, check=True)

    def configured(self):
        values = signing.configure()
        os.environ.update(values)
        self.assertNotIn("RELEASE_GPG_PASSPHRASE_FILE", values)
        self.assertFalse((Path(values["RELEASE_SIGNING_DIR"]) / "passphrase").exists())
        self.assertNotIn(self.password, Path(os.environ["GITHUB_ENV"]).read_text())
        return values

    def release(self):
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
        # Maven gitexe creates annotated tags with -F rather than an explicit -s.
        Path("message").write_text("Release 1.0\n")
        signing.run(["git", "tag", "-F", "message", "1.0"])
        signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Next development"])

    def test_signed_release_and_cloned_checkout(self):
        with self.repository() as directory:
            values = self.configured()
            self.assertIn("-Dgpg.keyname=" + self.subkey + "!", values["RELEASE_MAVEN_OPTS"])
            self.release()
            signing.verify_release("1.0")
            # release:perform makes a separate clone: signing must survive that boundary.
            clone = directory / "perform-clone"
            signing.run(["git", "clone", "--quiet", "--local", ".", str(clone)])
            signing.run(["git", "-C", str(clone), "commit", "--quiet", "--allow-empty",
                         "-m", "Clone signing"])
            status = signing.run(["git", "-C", str(clone), "-c", "gpg.program=gpg",
                                  "verify-commit", "--raw", "HEAD"],
                                 signing.public_environment(Path(values["RELEASE_SIGNING_DIR"])))
            signing.verify_status(status)
            signing.cleanup()
            self.assertFalse(Path(values["RELEASE_SIGNING_DIR"]).exists())

    def test_empty_passphrase_fails_before_configuration(self):
        with self.repository(password="") as directory:
            with self.assertRaisesRegex(signing.SigningError, "nonempty"):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_wrong_passphrase_fails_with_fresh_agent(self):
        with self.repository(password="wrong-test-passphrase") as directory:
            with self.assertRaises(signing.SigningError):
                signing.configure()
            self.assertFalse(list(directory.glob("release-signing-*")))

    def test_primary_secret_rejected(self):
        with self.repository(full=True):
            with self.assertRaisesRegex(signing.SigningError, "signing-private.asc"):
                signing.configure()

    def test_unsigned_release_commit_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                         "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_unsigned_release_tag_rejected(self):
        with self.repository():
            self.configured()
            self.release()
            signing.run(["git", "tag", "-d", "1.0"])
            signing.run(["git", "-c", "tag.gpgSign=false", "tag", "-a", "-m", "Unsigned",
                         "1.0", "HEAD~1"])
            with self.assertRaises(signing.SigningError):
                signing.verify_release("1.0")

    def test_tag_at_unrelated_commit_rejected(self):
        with self.repository():
            self.configured()
            signing.run(["git", "tag", "-m", "Old commit", "1.0"])
            signing.run(["git", "commit", "--quiet", "--allow-empty", "-m", "Release"])
            with self.assertRaisesRegex(signing.SigningError, "newly signed"):
                signing.verify_release("1.0")

    def test_public_only_tag_and_commit_verification(self):
        with self.repository():
            self.configured()
            self.release()
            signing.verify_tag("1.0")
            signing.verify_commit("HEAD")
            signing.run(["git", "-c", "commit.gpgsign=false", "commit", "--quiet",
                         "--allow-empty", "-m", "Unsigned"])
            with self.assertRaises(signing.SigningError):
                signing.verify_commit("HEAD")
            with self.assertRaises(signing.SigningError):
                signing.verify_tag("1.0", signing.run(["git", "rev-parse", "HEAD"]).strip())

    def test_detached_signature_reuse_and_tamper_rejection(self):
        with self.repository():
            self.configured()
            artifact = Path("package.tar.gz")
            artifact.write_bytes(b"release package")
            signing.sign_files([str(artifact)])
            signature = Path(str(artifact) + ".asc").read_bytes()
            signing.sign_files([str(artifact)])
            self.assertEqual(Path(str(artifact) + ".asc").read_bytes(), signature)
            artifact.write_bytes(b"changed package")
            with self.assertRaises(signing.SigningError):
                signing.sign_files([str(artifact)])

    def test_pipeline_preparation_recreates_candidate_without_signing_credentials(self):
        with self.repository() as directory:
            Path("Cargo.toml").write_text('[package]\nname = "alternator-client"\nversion = "1.0.0"\n')
            Path("Cargo.lock").write_text('version = 4\n\n[[package]]\nname = "alternator-client"\nversion = "1.0.0"\n')
            Path("CHANGELOG.md").write_text(
                '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- Bug fix.\n\n'
                '## [1.0.0] - 2026-10-02\n\n- Initial release.\n\n'
                '[Unreleased]: https://github.com/scylladb/alternator-client-rust/compare/v1.0.0...HEAD\n')
            signing.run(["git", "add", "."])
            signing.run(["git", "-c", "user.name=Initial", "-c", "user.email=initial@example.invalid",
                         "-c", "commit.gpgsign=false", "commit", "--amend", "--no-edit"])
            signing.run(["git", "branch", "-M", "main"])
            source = signing.run(["git", "rev-parse", "HEAD"]).strip()
            origin = directory / "origin.git"
            signing.run(["git", "clone", "--bare", ".", str(origin)])
            signing.run(["git", "remote", "add", "origin", str(origin)])
            os.environ.update(GITHUB_SHA=source, GITHUB_REF="refs/heads/main", GITHUB_RUN_ID="1234",
                              GITHUB_RUN_ATTEMPT="1", RELEASE_TIMESTAMP="2026-10-25T12:00:00Z")
            prepare_release.prepare("1.0.1")
            commit = signing.run(["git", "rev-parse", "HEAD"]).strip()
            self.assertNotEqual(commit, source)
            self.assertEqual(signing.run(["git", "show", "-s", "--format=%P", commit]).strip(), source)
            with self.assertRaises(signing.SigningError):
                signing.verify_commit(commit)
            self.assertIn('version = "1.0.1"', Path("Cargo.toml").read_text())
            self.assertIn('version = "1.0.1"', Path("Cargo.lock").read_text())
            self.assertIn('## [1.0.1] - ', Path("CHANGELOG.md").read_text())
            self.assertFalse(signing.run(["git", "status", "--porcelain"]).strip())
            allocation = subprocess.run(
                ["bash", str(Path(__file__).with_name("allocate-rc.sh")), "--dry-run",
                 "1.0.1", commit], capture_output=True, text=True, check=True,
                env={**os.environ, "GITHUB_OUTPUT": str(directory / "allocation-output")},
            )
            self.assertIn("v1.0.1-rc.1", allocation.stdout)
            self.assertFalse(signing.run(["git", "tag", "-l"]).strip())
            signing.run(["git", "checkout", "--detach", source])
            prepare_release.prepare("1.0.1", commit)
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]).strip(), commit)
            signing.run(["git", "checkout", "--detach", source])
            with self.assertRaisesRegex(ValueError, "differs from the verified candidate"):
                prepare_release.prepare("1.0.1", "0" * 40)
            # Approval unlocks the key only after this exact commit is tested.
            self.configured()
            signing.run(["git", "tag", "-s", "v1.0.1-rc.1", "-m", "RC\n\nworkflow-run: 1234"])
            signing.run(["git", "push", "origin", "refs/tags/v1.0.1-rc.1"])
            signing.run(["git", "checkout", "--detach", source])
            prepare_release.prepare("1.0.1")
            self.assertEqual(signing.run(["git", "rev-parse", "HEAD"]).strip(), commit)
            signing.run(["git", "fetch", "origin", "main"])
            self.assertEqual(signing.run(["git", "rev-parse", "origin/main"]).strip(), source)
            os.environ["GITHUB_SHA"] = "0" * 40
            with self.assertRaisesRegex(ValueError, "child"):
                prepare_release.prepare("1.0.1")

    def test_import_from_protected_files_and_local_exec_restores_config(self):
        with self.repository() as directory:
            config = Path(signing.run(["git", "rev-parse", "--git-path", "config"]).strip())
            before = config.read_bytes()
            os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"] = str(self.private)
            with patch("sys.argv", ["signing.py", "exec", "git", "commit", "--quiet",
                                    "--allow-empty", "-m", "Signed by isolated exec"]):
                self.assertEqual(signing.main(), 0)
            self.assertEqual(config.read_bytes(), before)
            self.assertFalse(list(directory.glob("release-signing-*")))
            signing.verify_commit("HEAD")

    def test_git_wrapper_reads_current_passphrase_after_agent_reset(self):
        with self.repository():
            self.configured()
            signing.run(["gpgconf", "--homedir", os.environ["GNUPGHOME"], "--kill", "gpg-agent"])
            os.environ["MAVEN_GPG_PASSPHRASE"] = "incorrect-fixture-passphrase"
            with self.assertRaises(signing.SigningError):
                signing.run(["git", "commit", "--allow-empty", "-qm", "Must fail"])
            os.environ["MAVEN_GPG_PASSPHRASE"] = self.password
            signing.run(["git", "commit", "--allow-empty", "-qm", "Correct passphrase"])
            signing.verify_commit("HEAD")

    def test_other_signer_and_ambiguous_signatures_rejected(self):
        good = "[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Release Test"
        valid_signature = "[GNUPG:] VALIDSIG " + signing.SIGNING + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY
        valid = good + "\n" + valid_signature
        signing.verify_status(valid)
        for status in [valid.replace(signing.SIGNING, "0" * 40),
                       valid.replace(signing.PRIMARY, "0" * 40), valid + "\n" + valid,
                       valid_signature, good, valid + "\n" + good]:
            with self.assertRaises(signing.SigningError):
                signing.verify_status(status)

    def test_expired_revoked_and_bad_signature_status_rejected(self):
        valid = ("[GNUPG:] GOODSIG " + signing.SIGNING[-16:] + " Release Test\n"
                 "[GNUPG:] VALIDSIG " + signing.SIGNING
                 + " 2026-10-20 1 0 4 0 1 10 00 " + signing.PRIMARY)
        for record in ["EXPKEYSIG", "REVKEYSIG", "EXPSIG", "BADSIG", "ERRSIG"]:
            with self.subTest(record=record), self.assertRaises(signing.SigningError):
                signing.verify_status(valid + "\n[GNUPG:] " + record + " rejected")

    def test_repository_isolates_inherited_signing_secrets(self):
        inherited = {"RELEASE_GPG_PRIVATE_KEY_FILE": "/missing/private.asc",
                     "RELEASE_GPG_PASSPHRASE_FILE": "/missing/passphrase",
                     "RELEASE_GPG_PRIVATE_KEY": "invalid inherited key",
                     "RELEASE_GPG_PASSPHRASE": "invalid inherited passphrase",
                     "MAVEN_GPG_PASSPHRASE": "invalid inherited Maven passphrase",
                     "PGP_PASSPHRASE": "invalid inherited PGP passphrase"}
        with patch.dict(os.environ, inherited), self.repository():
            for key in inherited:
                if key != "MAVEN_GPG_PASSPHRASE":
                    self.assertNotIn(key, os.environ)
            self.assertEqual(os.environ["MAVEN_GPG_PASSPHRASE"], self.password)
            self.configured()


if __name__ == "__main__":
    unittest.main()
