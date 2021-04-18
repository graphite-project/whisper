#!/usr/bin/env python

import os
from glob import glob
from setuptools import setup


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


setup(
  name='whisper',
  version='1.1.8',
  url='http://graphiteapp.org/',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Fixed size round-robin style database',
  long_description=read('README.md'),
  long_description_content_type='text/markdown',
  py_modules=['whisper'],
  scripts=glob('bin/*') + glob('contrib/*'),
  install_requires=['six'],
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: Implementation :: CPython',
    'Programming Language :: Python :: Implementation :: PyPy',
  ],
  zip_safe=False
)
