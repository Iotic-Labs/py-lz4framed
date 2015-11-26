# Copyright (c) 2015 Iotic Labs Ltd. All rights reserved.

from distutils.core import setup, Extension

VERSION = '0.7'
# see lz4/NEWS
LZ4_VERSION = 'r131'

setup(
    name='py-lz4framed',
    version=VERSION,
    description='LZ4Frame library for Python (via C bindings)',
    author='Iotic Labs Ltd.',
    author_email='info@iotic-labs.com',
    maintainer='Vilnis Termanis',
    maintainer_email='vilnis.termanis@iotic-labs.com',
    # Change this if/when making public (this is a required field), also add license field
    url='https://github.com/Iotic-Labs/py-lz4framed',
    # TODO - set once making public
    # license='???',
    packages=['lz4framed'],
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
            '-DVERSION="%s"' % VERSION,
            '-DLZ4_VERSION="%s"' % LZ4_VERSION,
            # TODO - some of these are GCC-specific
            '-O3',
            '-Wall',
            '-Wextra',
            '-Wundef',
            '-Wshadow',
            '-Wcast-align',
            '-Wcast-qual',
            '-Wstrict-prototypes',
            '-pedantic'
        ])],
    keywords=('lz4framed', 'lz4frame', 'lz4'),
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        # Add license classifier here if available in classifiers list
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    )
)
