#!/usr/bin/env python

import os
from glob import glob
from distutils.core import setup


setup(
  name='whisper',
  version='1.0.2',
  url='http://graphiteapp.org/',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Fixed size round-robin style database',
  py_modules=['whisper'],
  scripts=glob('bin/*') + glob('contrib/*'),
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: Implementation :: CPython',
    'Programming Language :: Python :: Implementation :: PyPy',
  ],
)
