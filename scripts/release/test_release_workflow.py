"""Enforce the verification, approval, and credential boundaries in release CI."""

import json
from pathlib import Path
import unittest

import yaml


class ReleaseWorkflowTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[2]
        self.workflow = yaml.safe_load((root / '.github/workflows/release.yml').read_text())
        self.jobs = self.workflow['jobs']

    def test_only_release_jobs_receive_secrets_and_environment(self):
        self.assertNotIn('secrets.', json.dumps(self.workflow.get('env', {})))
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                if name in ('preflight', 'publish'):
                    self.assertEqual(job['environment'], 'release')
                else:
                    self.assertNotIn('environment', job)
                    self.assertNotIn('secrets.', json.dumps(job))
                    self.assertNotEqual(job.get('permissions', {}).get('contents'), 'write')
        source = self.jobs['verify_source']
        self.assertEqual(source['uses'], './.github/workflows/ci.yml')
        self.assertNotIn('secrets', source)

    def test_preparation_approval_follows_source_verification(self):
        prepare = self.jobs['preflight']
        self.assertEqual(prepare['needs'], 'verify_source')
        scripts = '\n'.join(s.get('run', '') for s in prepare['steps'])
        self.assertLess(scripts.index('prepare_release.py'), scripts.index('preflight.sh'))
        self.assertLess(scripts.index('preflight.sh'), scripts.index('allocate-rc.sh'))
        self.assertIn('signing.py cleanup', scripts)

    def test_publication_approval_follows_all_candidate_gates(self):
        self.assertIn('final_gate', self.jobs['publish']['needs'])
        self.assertIn('evidence', self.jobs['publish']['needs'])
        self.assertEqual(set(self.jobs['final_gate']['needs']),
                         {'portable_gate', 'static_release_gate', 'scylla_gate', 'evidence'})
        self.assertNotIn('if', self.jobs['publish'])
        scripts = '\n'.join(s.get('run', '') for s in self.jobs['publish']['steps'])
        self.assertLess(scripts.index('repackage-and-compare.sh'), scripts.index('publish-and-verify.sh'))
        self.assertLess(scripts.index('publish-and-verify.sh'), scripts.index('finalize-release.sh'))
        self.assertIn('signing.py cleanup', scripts)
        self.assertNotIn('publish locally', scripts)


if __name__ == '__main__':
    unittest.main()
