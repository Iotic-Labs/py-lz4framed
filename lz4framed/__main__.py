# Copyright (c) 2015 Iotic Labs Ltd. All rights reserved.

"""(de)compresses to/from lz4-framed data"""

from __future__ import print_function
from sys import argv, stderr

from .compat import stdin_raw, stdout_raw
from . import Compressor, Decompressor, Lz4FramedError, Lz4FramedNoDataError, get_block_size


def __error(*args, **kwargs):
    print(*args, file=stderr, **kwargs)


def do_compress(inStream, outStream):
    read = inStream.read
    readSize = get_block_size()
    try:
        with Compressor(fp=outStream) as c:
            try:
                while True:
                    c.update(read(readSize))
            # empty read result supplied to update()
            except Lz4FramedNoDataError:
                pass
            # input stream exception
            except EOFError:
                pass
    except Lz4FramedError as e:
        __error('Compression error: %s' % e)
        return 8
    return 0


def do_decompress(inStream, outStream):
    write = outStream.write
    try:
        for chunk in Decompressor(inStream):
            write(chunk)
    except Lz4FramedError as e:
        __error('Compression error: %s' % e)
        return 8
    return 0


__action = frozenset(('compress', 'decompress'))


def main():  # noqa (complexity)
    if not (3 <= len(argv) <= 4 and argv[1] in __action):
        print("""USAGE: lz4framed (compress|decompress) (INFILE|-) [OUTFILE]

(De)compresses an lz4 frame. Input is read from INFILE unless set to '-', in
which case stdin is used. If OUTFILE is not specified, output goes to stdout.""", file=stderr)
        return 1

    compress = (argv[1] == 'compress')
    inFile = outFile = None
    try:
        # input
        if argv[2] == '-':
            inStream = stdin_raw
        else:
            try:
                inStream = inFile = open(argv[2], 'rb')
            except IOError as e:
                __error('Failed to open input file for reading: %s' % e)
                return 2
        # output
        if len(argv) == 3:
            outStream = stdout_raw
        else:
            try:
                outStream = outFile = open(argv[2], 'ab')
            except IOError as e:
                __error('Failed to open output file for appending: %s' % e)
                return 4

        return (do_compress if compress else do_decompress)(inStream, outStream)
    except IOError as e:
        __error('I/O failure: %s' % e)
    finally:
        if inFile:
            inFile.close()
        if outFile:
            outFile.close()


if __name__ == "__main__":
    exit(main())
