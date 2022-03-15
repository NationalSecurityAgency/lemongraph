from __future__ import print_function
import os
import platform
import stat
import subprocess
import sys

from setuptools import setup
from distutils.command.build import build
from distutils.command.sdist import sdist
from setuptools.command.install import install

def fetch_external():
    try:
        return fetch_external.once
    except AttributeError:
        pass
    subprocess.check_call('make --no-print-directory deps'.split())
    setattr(fetch_external, 'once', None)

def Wrap(cls):
    class Wrapper(cls):
        def run(self):
            fetch_external()
            return cls.run(self)
    return Wrapper

def wrap(**classes):
    for label, cls in classes.items():
        classes[label] = Wrap(cls)
    return classes
#    return dict((cls.__module__.split('.')[-1], Wrap(cls)) for cls in classes)

cffi = ['cffi>=1.8.2,<1.13']
reqs = cffi + [
    'lazy>=1.0,<1.5',
    'msgpack>=0.6,<0.7',
    'python-dateutil>=1.4,<2.9',
    'setuptools',
    'six',
]

if platform.python_implementation() == 'CPython':
    reqs.append('ujson')

if __name__ == "__main__":
    setup(
        name='LemonGraph',
        maintainer='National Security Agency',
        maintainer_email='/dev/null',
        url='https://github.com/NationalSecurityAgency/lemongraph',
        version='3.2.5',
        description='LemonGraph Database',
        packages=['LemonGraph', 'LemonGraph.server'],
        package_data={'LemonGraph': ['data/*']},
        install_requires=reqs,
        setup_requires=cffi,
        cffi_modules=['LemonGraph/cffi_stubs.py:ffi'],
        cmdclass=wrap(build=build, install=install, sdist=sdist))
