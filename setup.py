#!/usr/bin/env python

from glob import glob
from setuptools import setup, find_packages


setup(
  name='whisper',
  version='1.2.0',
  url='http://graphiteapp.org/',
  author='Chris Davis',
  author_email='chrismd@gmail.com',
  license='Apache Software License 2.0',
  description='Fixed size round-robin style database',
  packages=find_packages(),
  entry_points={
    'console_scripts': [
      'rrd2whisper.py = whisper.cli.rrd2whisper:main',
      'whisper-create.py = whisper.cli.create:main',
      'whisper-diff.py = whisper.cli.diff:main',
      'whisper-dump.py = whisper.cli.dump:main',
      'whisper-fetch.py = whisper.cli.fetch:main',
      'whisper-fill.py = whisper.cli.fill:main',
      'whisper-info.py = whisper.cli.info:main',
      'whisper-merge.py = whisper.cli.merge:main',
      'whisper-resize.py = whisper.cli.resize:main',
      'whisper-set-aggregation-method.py = whisper.cli.set_aggregation_method:main',
      'whisper-set-xfilesfactor.py = whisper.cli.set_xfilesfactor:main',
      'whisper-update.py = whisper.cli.update:main',
      'whisper-auto-update.py = whisper.contrib.auto_update:main',
      'whisper-auto-resize.py = whisper.contrib.auto_resize:main',
      'update-storage-times.py = whisper.contrib.update_storage_times:main'
    ]
  },
  scripts=glob('contrib/*'),
  install_requires=['six'],
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: Implementation :: CPython',
    'Programming Language :: Python :: Implementation :: PyPy',
  ],
  zip_safe=False
)
