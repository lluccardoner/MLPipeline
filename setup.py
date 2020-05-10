#!/usr/bin/env python

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
  long_description = f.read()

setup(
  name='ml-pipeline',
  version='0.1',
  description='Framework to create Spark pipelines without coding, just by using json config files',
  long_description=long_description,
  author='Lluc Cardoner',
  author_email='lluccardoner',
  url='https://github.com/lluccardoner/MLPipeline',

  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Data Scientists',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8'
  ],
  python_requires='>=3.5',

  install_requires=[
    'pyspark',
    'numpy'
  ],

  extras_require={
    'dev': [
      'ipdb',
      'pytest',
      'pytest-pep8',
      'pytest-xdist',
      'flaky',
      'pytest-cov',
      'pytest-sugar'
    ]
  },

  package_dir={'': 'src'},

  packages=find_packages(where='src'),
)
