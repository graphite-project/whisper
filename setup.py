#!/usr/bin/env python

import os
from glob import glob
from distutils.core import setup


setup(
  name='whisper',
  version='0.9.10',
  url='https://launchpad.net/graphite',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Fixed size round-robin style database',
  py_modules=['whisper'],
  scripts=glob('bin/*'),
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
  ],
)
