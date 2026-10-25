"""Exercise draft staging and publication failures without network mutations."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
COMMIT = 'a' * 40
RC_TAG = 'v1.0.1-rc.1'
FINAL_TAG = 'v1.0.1'

# GitHub and Git state live outside the source checkout. Real jq, cmp, Bash,
# and filesystem operations exercise the finalizer's validation and branching.
FAKE_COMMAND = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

state_path = Path(os.environ['RELEASE_TEST_STATE'])
state = json.loads(state_path.read_text())
command = Path(sys.argv[0]).name
args = sys.argv[1:]
state['calls'].append([command, *args])
state_path.write_text(json.dumps(state))

def save():
    state_path.write_text(json.dumps(state))

def value(flag):
    return args[args.index(flag) + 1]

def asset_by_id(asset_id):
    return next(a for a in state['release']['assets'] if str(a['id']) == asset_id)

if command == 'git':
    if args[0] == 'fetch':
        pass
    elif args[0] == 'rev-parse':
        tag = args[-1].removeprefix('refs/tags/')
        if tag not in state['tags']:
            sys.exit(1)
        print(state['tags'][tag])
    elif args[0] == 'cat-file':
        print('tag')
    elif args[0] == 'rev-list':
        print(state['tags'][args[-1]])
    elif args[0] == 'ls-remote':
        tag = args[-1].removeprefix('refs/tags/').removesuffix('^{}')
        if tag in state['remote_tags']:
            print(state['remote_tags'][tag] + '\t' + args[-1])
    elif args[0] == 'tag':
        state['tags'][args[2]] = args[3]
        save()
    elif args[0] == 'push':
        tag = args[-1].removeprefix('refs/tags/')
        state['remote_tags'][tag] = state['tags'][tag]
        save()
    else:
        raise AssertionError(args)
elif command == 'gh':
    if args[0] == 'api':
        endpoint = args[-1]
        if '/releases?' in endpoint:
            print(json.dumps([[state['release']] if state['release'] else []]))
        elif '/releases/assets/' in endpoint and 'DELETE' in args:
            asset = asset_by_id(endpoint.rsplit('/', 1)[-1])
            state['release']['assets'].remove(asset)
            save()
        elif '/releases/' in endpoint:
            print(json.dumps(state['release']))
        else:
            raise AssertionError(args)
    elif args[:2] == ['release', 'create']:
        state['release'] = {
            'id': 1, 'tag_name': args[2], 'target_commitish': value('--target'),
            'name': value('--title'), 'body': Path(value('--notes-file')).read_text(),
            'draft': True, 'immutable': False, 'assets': [],
            'html_url': 'https://example.invalid/release',
        }
        save()
    elif args[:2] == ['release', 'upload']:
        path = Path(args[-1])
        state['release']['assets'].append({
            'id': state['next_id'], 'name': path.name, 'state': 'uploaded',
            'content': path.read_text(),
        })
        state['next_id'] += 1
        save()
    elif args[:2] == ['release', 'download']:
        destination = Path(value('--dir'))
        destination.mkdir(exist_ok=True)
        for asset in state['release']['assets']:
            if '--pattern' not in args or asset['name'] == value('--pattern'):
                (destination / asset['name']).write_text(asset['content'])
    elif args[:2] == ['release', 'edit']:
        assert state['remote_tags'].get(args[2]) == state['release']['target_commitish']
        state['release'].update(draft=False, immutable=True)
        save()
    else:
        raise AssertionError(args)
elif command == 'curl':
    asset = asset_by_id(args[-1].rsplit('/', 1)[-1])
    content = asset['content']
    if os.environ.get('RELEASE_TEST_CORRUPT_DOWNLOAD') == asset['name']:
        content += 'corruption'
    Path(value('-o')).write_text(content)
else:
    raise AssertionError(command)
'''

FAKE_SIGNING = r'''import hashlib
from pathlib import Path
import sys

if sys.argv[1] == 'sign-files':
    for filename in sys.argv[2:]:
        path = Path(filename)
        signature = Path(filename + '.asc')
        expected = hashlib.sha256(path.read_bytes()).hexdigest()
        if signature.exists():
            if signature.read_text() != expected:
                sys.exit('invalid existing signature')
        else:
            signature.write_text(expected)
else:
    assert sys.argv[1] == 'verify-tag', sys.argv
'''


@unittest.skipUnless(shutil.which('jq'), 'jq required')
class FinalizeReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='release-finalization-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / 'scripts/release'
        scripts.mkdir(parents=True)
        shutil.copy(ROOT / 'scripts/release/finalize-release.sh', scripts)
        (scripts / 'signing.py').write_text(FAKE_SIGNING)
        self.executable(scripts / 'verify-candidate.sh', '#!/bin/bash\nexit 0\n')
        self.executable(scripts / 'check-release-blockers.sh', '#!/bin/bash\nexit 0\n')
        self.executable(scripts / 'registry-state.sh', '''#!/bin/bash
printf 'state=%s\n' "$RELEASE_TEST_REGISTRY" >>"$GITHUB_OUTPUT"
[[ "$RELEASE_TEST_REGISTRY" != conflict ]]
''')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name in ('git', 'gh', 'curl'):
            self.executable(self.bin / name, FAKE_COMMAND)
        self.state_path = self.root / 'state.json'
        self.state_path.write_text(json.dumps({
            'release': None, 'tags': {RC_TAG: COMMIT},
            'remote_tags': {RC_TAG: COMMIT}, 'next_id': 1, 'calls': [],
        }))
        self.candidate = self.root / 'candidate'
        self.candidate.mkdir()
        (self.candidate / 'alternator-client-1.0.1.crate').write_text('tested crate')
        (self.candidate / 'alternator-client-1.0.1.cdx.json').write_text('tested SBOM')
        (self.candidate / 'SHA256SUMS').write_text('tested checksums')
        (self.candidate / 'release-manifest.json').write_text(json.dumps({
            'crate': {'file': 'alternator-client-1.0.1.crate', 'sha256': 'b' * 64},
            'sbom': {'file': 'alternator-client-1.0.1.cdx.json'},
        }))
        self.evidence = self.root / 'test-evidence.json'
        self.evidence.write_text(json.dumps({
            'rc_tag': RC_TAG, 'commit_sha': COMMIT, 'all_required_gates_passed': True,
        }))
        (self.root / 'CHANGELOG.md').write_text('## [1.0.1] - 2026-10-25\n\n- Fix.\n')

    def executable(self, path, content):
        path.write_text(content)
        path.chmod(0o755)

    def state(self):
        return json.loads(self.state_path.read_text())

    def update_state(self, state):
        self.state_path.write_text(json.dumps(state))

    def run_phase(self, phase, registry='absent', corrupt=''):
        return subprocess.run(
            ['bash', 'scripts/release/finalize-release.sh', phase, '1.0.1', RC_TAG,
             COMMIT, str(self.candidate), str(self.evidence)],
            cwd=self.root, capture_output=True, text=True, timeout=30,
            env={**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
                 'GITHUB_REPOSITORY': 'test/release', 'GH_TOKEN': 'test-only',
                 'RELEASE_TEST_STATE': str(self.state_path), 'RELEASE_TEST_REGISTRY': registry,
                 'RELEASE_TEST_CORRUPT_DOWNLOAD': corrupt},
        )

    def prepare(self):
        result = self.run_phase('prepare')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_prepare_verifies_all_assets_without_publication_or_final_tag(self):
        self.prepare()
        state = self.state()
        self.assertTrue(state['release']['draft'])
        self.assertEqual(len(state['release']['assets']), 10)
        self.assertEqual(state['release']['target_commitish'], COMMIT)
        self.assertNotIn(FINAL_TAG, state['tags'])
        self.assertEqual(sum(c[0] == 'curl' for c in state['calls']), 10)
        self.assertFalse(any(c[:3] == ['gh', 'release', 'edit'] for c in state['calls']))
        self.prepare()  # Retry must reuse every asset and signature.
        self.assertEqual(sum(c[:3] == ['gh', 'release', 'upload']
                             for c in self.state()['calls']), 10)

    def test_corrupt_draft_download_blocks_preparation(self):
        result = self.run_phase('prepare', corrupt='alternator-client-1.0.1.crate')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('differs from its canonical local file', result.stderr)
        self.assertTrue(self.state()['release']['draft'])
        self.assertNotIn(FINAL_TAG, self.state()['tags'])

    def test_registry_absence_or_conflict_blocks_finalization(self):
        self.prepare()
        for registry in ('absent', 'conflict'):
            with self.subTest(registry=registry):
                result = self.run_phase('publish', registry)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(self.state()['release']['draft'])
                self.assertNotIn(FINAL_TAG, self.state()['tags'])

    def test_already_published_registry_blocks_draft_creation(self):
        result = self.run_phase('prepare', registry='exact')
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(self.state()['release'])
        self.assertNotIn(FINAL_TAG, self.state()['tags'])

    def test_publication_requires_prepared_draft(self):
        result = self.run_phase('publish', registry='exact')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('verified draft GitHub Release is missing', result.stderr)
        self.assertIsNone(self.state()['release'])
        self.assertNotIn(FINAL_TAG, self.state()['tags'])

    def test_missing_or_modified_asset_is_never_repaired_during_publication(self):
        self.prepare()
        initial = self.state()
        for damage in ('missing', 'modified', 'starter'):
            with self.subTest(damage=damage):
                state = json.loads(json.dumps(initial))
                if damage == 'missing':
                    state['release']['assets'].pop(0)
                elif damage == 'modified':
                    state['release']['assets'][0]['content'] = 'different crate'
                else:
                    state['release']['assets'][0]['state'] = 'starter'
                state['calls'] = []
                self.update_state(state)
                result = self.run_phase('publish', registry='exact')
                self.assertNotEqual(result.returncode, 0)
                state = self.state()
                self.assertTrue(state['release']['draft'])
                self.assertNotIn(FINAL_TAG, state['tags'])
                self.assertFalse(any(c[:3] == ['gh', 'release', 'upload'] for c in state['calls']))

    def test_exact_registry_finalizes_and_retries_without_uploading(self):
        self.prepare()
        state = self.state()
        state['calls'] = []
        self.update_state(state)
        for _ in range(2):
            result = self.run_phase('publish', registry='exact')
            self.assertEqual(result.returncode, 0, result.stderr)
        state = self.state()
        self.assertFalse(state['release']['draft'])
        self.assertTrue(state['release']['immutable'])
        self.assertEqual(state['remote_tags'][FINAL_TAG], COMMIT)
        self.assertFalse(any(c[:3] == ['gh', 'release', 'upload'] for c in state['calls']))
        self.assertEqual(sum(c[:3] == ['gh', 'release', 'edit'] for c in state['calls']), 1)


if __name__ == '__main__':
    unittest.main()
