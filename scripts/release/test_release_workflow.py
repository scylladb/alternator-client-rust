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
                if name == 'publish':
                    self.assertEqual(job['environment'], 'release')
                else:
                    self.assertNotIn('environment', job)
                    self.assertNotIn('secrets.', json.dumps(job))
                    self.assertNotEqual(job.get('permissions', {}).get('contents'), 'write')
        source = self.jobs['verify_source']
        self.assertEqual(source['uses'], './.github/workflows/ci.yml')
        self.assertNotIn('secrets', source)

    def test_preparation_needs_no_release_credentials_or_writes(self):
        prepare = self.jobs['preflight']
        self.assertEqual(prepare['needs'], 'verify_source')
        scripts = '\n'.join(s.get('run', '') for s in prepare['steps'])
        self.assertLess(scripts.index('prepare_release.py'), scripts.index('preflight.sh'))
        self.assertLess(scripts.index('preflight.sh'), scripts.index('allocate-rc.sh'))
        self.assertIn('allocate-rc.sh --dry-run', scripts)
        self.assertNotIn('signing.py configure', scripts)
        self.assertNotIn('environment', prepare)
        self.assertEqual(prepare['permissions']['contents'], 'read')

    def test_candidate_commits_are_recreated_from_pinned_dispatch(self):
        for name in ('package', 'publish'):
            with self.subTest(job=name):
                steps = self.jobs[name]['steps']
                checkout = next(s for s in steps if 'actions/checkout@' in s.get('uses', ''))
                self.assertEqual(checkout['with']['ref'], '${{ github.sha }}')
                recreate = next(s for s in steps if 'prepare_release.py' in s.get('run', ''))
                self.assertIn('needs.preflight.outputs.commit_sha', recreate['run'])
                self.assertEqual(recreate['env']['RELEASE_TIMESTAMP'],
                                 '${{ needs.preflight.outputs.release_timestamp }}')

    def test_publication_approval_follows_all_candidate_gates(self):
        self.assertIn('final_gate', self.jobs['publish']['needs'])
        self.assertIn('evidence', self.jobs['publish']['needs'])
        self.assertEqual(set(self.jobs['final_gate']['needs']),
                         {'portable_gate', 'static_release_gate', 'scylla_gate', 'evidence'})
        self.assertNotIn('if', self.jobs['publish'])
        scripts = '\n'.join(s.get('run', '') for s in self.jobs['publish']['steps'])
        self.assertLess(scripts.index('repackage-and-compare.sh'), scripts.index('publish-and-verify.sh'))
        self.assertLess(scripts.index('finalize-release.sh prepare'), scripts.index('publish-and-verify.sh'))
        self.assertLess(scripts.index('publish-and-verify.sh'), scripts.index('finalize-release.sh publish'))
        self.assertIn('signing.py cleanup', scripts)
        self.assertNotIn('publish locally', scripts)
        steps = self.jobs['publish']['steps']
        draft = next(i for i, s in enumerate(steps) if 'finalize-release.sh prepare' in s.get('run', ''))
        auth = next(i for i, s in enumerate(steps) if 'crates-io-auth-action@' in s.get('uses', ''))
        evidence = next(i for i, s in enumerate(steps) if s['name'] == 'Download test evidence')
        self.assertLess(evidence, draft)
        self.assertLess(draft, auth)
        self.assertEqual(steps[draft]['if'], "${{ steps.registry.outputs.state == 'absent' }}")


if __name__ == '__main__':
    unittest.main()
