# Copyright (c) 2016 Iotic Labs Ltd. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://github.com/Iotic-Labs/py-lz4framed/blob/master/LICENSE
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=import-error,wrong-import-order,ungrouped-imports

from __future__ import print_function

import os

# Allow for environments without setuptools
try:
    from setuptools import setup, Extension
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Extension


def load_description(filename):
    script_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(script_dir, filename), 'r') as infile:
        return infile.read()


VERSION = '0.14.0'

setup(
    name='py-lz4framed',
    version=VERSION,
    description='LZ4Frame library for Python (via C bindings)',
    long_description=load_description('README.md'),
    long_description_content_type='text/markdown',
    author='Iotic Labs Ltd',
    author_email='info@iotic-labs.com',
    maintainer='Iotic Labs Ltd',
    maintainer_email='vilnis.termanis@iotic-labs.com',
    url='https://github.com/Iotic-Labs/py-lz4framed',
    license='Apache License 2.0',
    packages=['lz4framed'],
    zip_safe=False,
    ext_modules=[
        Extension('_lz4framed', [
            # lz4 library
            'lz4/lz4.c',
            'lz4/lz4hc.c',
            'lz4/lz4frame.c',
            'lz4/xxhash.c',
            'lz4framed/py-lz4framed.c',
        ], extra_compile_args=[
            '-Ilz4',
            '-std=c99',
            '-DXXH_NAMESPACE=PLZ4F_',
            '-DVERSION=%s' % VERSION,
            # For testing only - some of these are GCC-specific
            # '-Wall',
            # '-Wextra',
            # '-Wundef',
            # '-Wshadow',
            # '-Wcast-align',
            # '-Wcast-qual',
            # '-Wstrict-prototypes',
            # '-pedantic'
        ])],
    keywords=['lz4framed', 'lz4frame', 'lz4'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
