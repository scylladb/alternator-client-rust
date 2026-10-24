"""Check release version and changelog updates before creating a commit."""

from pathlib import Path
import tempfile
import unittest

import prepare_release


class VersionBumpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "Cargo.toml").write_text(
            '[package]\nname = "alternator-client"\nversion = "1.0.0"\n\n'
            '[dependencies]\nother = { version = "2.0.0" }\n')
        (self.root / "Cargo.lock").write_text(
            'version = 4\n\n[[package]]\nname = "alternator-client"\nversion = "1.0.0"\n\n'
            '[[package]]\nname = "other"\nversion = "2.0.0"\n')
        (self.root / "CHANGELOG.md").write_text(
            '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- Fix retry handling.\n\n'
            '## [1.0.0] - 2026-10-02\n\n- First release.\n\n'
            '[Unreleased]: https://github.com/scylladb/alternator-client-rust/compare/v1.0.0...HEAD\n'
            '[1.0.0]: https://github.com/scylladb/alternator-client-rust/releases/tag/v1.0.0\n')

    def files(self):
        return {p.name: p.read_text() for p in self.root.iterdir()}

    def test_bump_preserves_dependencies_and_historical_notes(self):
        prepare_release.bump_files(self.root, "1.1.0", "2026-10-23")
        files = self.files()
        self.assertIn('version = "1.1.0"', files['Cargo.toml'])
        self.assertIn('other = { version = "2.0.0" }', files['Cargo.toml'])
        self.assertIn('name = "alternator-client"\nversion = "1.1.0"', files['Cargo.lock'])
        self.assertIn('name = "other"\nversion = "2.0.0"', files['Cargo.lock'])
        self.assertIn('## [Unreleased]\n\n## [1.1.0] - 2026-10-23\n\n### Fixed', files['CHANGELOG.md'])
        self.assertIn('## [1.0.0] - 2026-10-02\n\n- First release.', files['CHANGELOG.md'])
        self.assertIn('/compare/v1.1.0...HEAD', files['CHANGELOG.md'])
        self.assertIn('[1.1.0]: https://github.com/scylladb/alternator-client-rust/compare/v1.0.0...v1.1.0', files['CHANGELOG.md'])

    def test_initial_version_updates_date_and_merges_unreleased_notes(self):
        prepare_release.bump_files(self.root, "1.0.0", "2026-10-23")
        text = (self.root / 'CHANGELOG.md').read_text()
        self.assertEqual(text.count('## [1.0.0]'), 1)
        self.assertIn('## [1.0.0] - 2026-10-23', text)
        self.assertIn('- First release.', text)
        self.assertIn('- Fix retry handling.', text)

    def test_invalid_versions_leave_files_unchanged(self):
        before = self.files()
        for version in ['0.9.0', '01.1.0', '1.0.1-rc.1', '1.0', '../1.0.0']:
            with self.subTest(version=version), self.assertRaises(ValueError):
                prepare_release.bump_files(self.root, version, '2026-10-23')
            self.assertEqual(self.files(), before)

    def test_new_version_requires_release_notes(self):
        file = self.root / 'CHANGELOG.md'
        file.write_text(file.read_text().replace('### Fixed\n\n- Fix retry handling.\n\n', ''))
        before = self.files()
        with self.assertRaisesRegex(ValueError, 'release notes'):
            prepare_release.bump_files(self.root, '1.0.1', '2026-10-23')
        self.assertEqual(self.files(), before)

    def test_lockfile_mismatch_leaves_files_unchanged(self):
        file = self.root / 'Cargo.lock'
        file.write_text(file.read_text().replace('version = "1.0.0"', 'version = "0.9.0"'))
        before = self.files()
        with self.assertRaisesRegex(ValueError, 'Cargo.lock'):
            prepare_release.bump_files(self.root, '1.0.1', '2026-10-23')
        self.assertEqual(self.files(), before)


if __name__ == '__main__':
    unittest.main()
