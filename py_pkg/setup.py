#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import scidbbridge


NAME = 'scidb-bridge'
DESCRIPTION = 'Python Library to access externally stored SciDB data'
LONG_DESCRIPTION = open('README.rst').read()
AUTHOR = 'Rares Vernica'
AUTHOR_EMAIL = 'rvernica@gmail.com'
DOWNLOAD_URL = 'http://github.com/Paradigm4/bridge'
LICENSE = 'AGPL-3.0'
VERSION = scidbbridge.__version__

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    download_url=DOWNLOAD_URL,
    license=LICENSE,
    packages=['scidbbridge'],
    install_requires=[
        'boto3>=1.14.12',
        'pyarrow==0.16.0',
        'scidb-py>=19.11.2',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Database :: Front-Ends',
        'Topic :: Scientific/Engineering',
    ],
)
