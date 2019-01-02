from __future__ import print_function
import os
import platform
import stat
import subprocess
import sys

from setuptools import setup

from lg_cffi_setup import keywords_with_side_effects


def git_submodule_init():
    lmdb_src = os.path.sep.join(('deps', 'lmdb'))
    try:
        if os.listdir(lmdb_src):
            return
    except OSError:
        os.mkdir(lmdb_src)
    lmdb_repo = 'https://github.com/LMDB/lmdb.git'
    try:
        if os.path.exists('.git'):
            subprocess.check_call(['git', 'submodule', 'init'])
            subprocess.check_call(['git', 'submodule', 'update'])
        else:
            subprocess.check_call(['git', 'clone', lmdb_repo, lmdb_src])
    except subprocess.CalledProcessError as e:
        cmd = ' '.join(e.cmd)
        raise RuntimeError('git cmd failed (%s) - please manually clone %s into %s' % (cmd, lmdb_repo, lmdb_src))


def do_curl(url):
    print("Fetching: %s" % url, file=sys.stderr)
    cmd = ('curl', '-L', url)
    p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE)
    print(url, file=sys.stderr)
    blen = 0
    for chunk in p.stdout:
        blen += len(chunk)
        yield chunk
    p.wait()
    if not blen or p.returncode:
        raise RuntimeError(
            'curl cmd failed (%s) - please manually fetch %s into deps (%d, %s)' % (cmd, url, blen, p.returncode))


def fetch_js():
    libs = {
        'd3.v3.min.js': 'https://d3js.org/d3.v3.min.js',
        'd3.v4.min.js': 'https://d3js.org/d3.v4.min.js',
        'svg-crowbar.js': 'https://nytimes.github.io/svg-crowbar/svg-crowbar.js',
    }
    for js, url in libs.items():
        target = os.path.sep.join(('LemonGraph', 'data', js))
        source = os.path.sep.join(('deps', 'js', js))
        dotsource = os.path.sep.join(('deps', 'js', '.%s' % js))
        try:
            os.mkdir(os.path.sep.join(('deps', 'js')))
        except OSError:
            pass

        try:
            s1 = os.stat(source)
        except OSError:
            with open(dotsource, 'wb') as fh:
                for chunk in do_curl(url):
                    fh.write(chunk)
                s1 = os.fstat(fh.fileno())
                fh.close()
                os.link(dotsource, source)
                os.unlink(dotsource)

        try:
            s2 = os.stat(target)
        except OSError:
            print("Hard linking: %s -> %s" % (source, target), file=sys.stderr)
            os.link(source, target)
            continue

        for i in (stat.ST_INO, stat.ST_DEV):
            if s1[i] != s2[i]:
                os.unlink(target)
                print("Hard linking: %s -> %s" % (source, target), file=sys.stderr)
                os.link(source, target)
                break


def fetch_external():
    git_submodule_init()
    fetch_js()


reqs = [
    'cffi>=1.8.2,<1.13',
    'lazy>=1.0,<1.4',
    'msgpack>=0.6,<0.7',
    'pysigset>=0.2,<0.4',
    'python-dateutil>=1.4,<2.7',
    'setuptools',
    'six',
]
if platform.python_implementation() == 'CPython':
    reqs.append('ujson')

if __name__ == "__main__":
    fetch_external()
    setup(
        name='LemonGraph',
        maintainer='National Security Agency',
        maintainer_email='/dev/null',
        url='https://github.com/NationalSecurityAgency/lemongraph',
        version='0.10.0',
        description='LemonGraph Database',
        packages=['LemonGraph', 'LemonGraph.server'],
        package_data={'LemonGraph': ['data/*']},
        install_requires=reqs,
        **keywords_with_side_effects(
            sys.argv,
            setup_requires=reqs[0:1],
            cffi_modules=['LemonGraph/cffi_stubs.py:ffi']))
