#!/usr/bin/env python3
"""Build a committed Core revision into the local RN dependency layout."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--platform', choices=['android', 'ios', 'all'], default='all')
    parser.add_argument('--prepare-only', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output).resolve()
    # Build only committed input, so a handoff never silently omits local changes.
    dirty = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=normal'], cwd=root, text=True)
    if dirty.strip():
        raise SystemExit('Commit changes or use a clean worktree before building.')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    output.mkdir(parents=True, exist_ok=False)
    source = output / 'source'
    source.mkdir()
    archive = subprocess.check_output(['git', 'archive', commit], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        for member in files:
            target = source / member.name
            if not target.resolve().is_relative_to(source):
                raise SystemExit('Unsafe archive path: ' + member.name)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
                with files.extractfile(member) as content:
                    target.write_bytes(content.read())
                target.chmod(member.mode)
            else:
                raise SystemExit('Unsupported archive entry: ' + member.name)
    rows = [{'path': str(p.relative_to(source)), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
            for p in sorted(source.rglob('*')) if p.is_file()]
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    version = '0.0.0-vlink.local.' + digest[:12]
    manifest = {'sourceCommit': commit, 'sourceDirty': False, 'sourceDigest': digest,
                'android': {'artifactVersion': version}}
    (output / 'source-input.json').write_text(json.dumps({'files': rows}, indent=2))
    (output / 'build-manifest.json').write_text(json.dumps(manifest, indent=2))
    shutil.copyfile(source / 'LICENSE', output / 'LICENSE')
    (output / 'OpenIMSDKCore.podspec').write_text("""Pod::Spec.new do |s|
  s.name = 'OpenIMSDKCore'
  s.version = '3.8.3-hotfix.12'
  s.summary = 'Local VLink Sync V2 Core'
  s.homepage = 'https://github.com/openimsdk/openim-sdk-core'
  s.license = { :type => 'AGPL-3.0-only', :file => 'LICENSE' }
  s.author = 'OpenIM'
  s.source = { :git => 'https://github.com/openimsdk/openim-sdk-core.git' }
  s.ios.deployment_target = '13.0'
  s.vendored_frameworks = 'OpenIMCore.xcframework'
  s.libraries = 'resolv'
end
""")
    if args.prepare_only:
        print(json.dumps(manifest))
        return
    platforms = ['android', 'ios'] if args.platform == 'all' else [args.platform]
    for platform in platforms:
        command = [os.environ.get('GOMOBILE', 'gomobile'), 'bind', '-trimpath']
        if platform == 'android':
            command += ['-ldflags=-s -w -extldflags "-Wl,-z,max-page-size=16384"',
                        '-target=android', '-androidapi=21', '-o', str(output / 'openim-core.aar')]
        else:
            command += ['-ldflags=-s -w', '-target=ios,iossimulator', '-iosversion=12.0',
                        '-o', str(output / 'OpenIMCore.xcframework')]
        command += ['./open_im_sdk/', './open_im_sdk_callback/']
        build_env = os.environ.copy()
        if platform == 'android':
            # Match Makefile: large per-conversation schemas need the larger hash allocation.
            build_env['CGO_CFLAGS'] = '-O2 -g -DSQLITE_MALLOC_SOFT_LIMIT=65536'
        with (output / (platform + '-bind.log')).open('w') as log:
            result = subprocess.run(command, cwd=source, env=build_env, stdout=log, stderr=subprocess.STDOUT)
        (output / (platform + '-build-result.json')).write_text(json.dumps({
            'platform': platform, 'sourceDigest': digest, 'exitCode': result.returncode,
            'command': command, 'diagnosticOnly': False}, indent=2))
        if result.returncode:
            raise SystemExit('Build failed; see ' + str(output / (platform + '-bind.log')))
        if platform == 'android':
            maven = output / 'maven/io/openim/core-sdk' / version
            maven.mkdir(parents=True)
            shutil.copyfile(output / 'openim-core.aar', maven / ('core-sdk-' + version + '.aar'))
            (maven / ('core-sdk-' + version + '.pom')).write_text(
                '<project><modelVersion>4.0.0</modelVersion><groupId>io.openim</groupId>'
                '<artifactId>core-sdk</artifactId><version>' + version + '</version><packaging>aar</packaging></project>')
    print(json.dumps(manifest))


if __name__ == '__main__':
    main()
