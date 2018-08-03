#!/usr/bin/env python

from glob import glob
from distutils.core import setup


setup(
  name='mem-whisper',
  version='1.2.0',
  url='https://github.com/goeuro/mem-whisper',
  author='Lorenzo Fundar',
  author_email='lorenzo.fundaro@goeuro.com',
  license='Apache Software License 2.0',
  description='Fork implementation taken from Chris Davis <chrismd@gmail.com>',
  py_modules=['mem-whisper'],
  install_requires=['six'],
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
