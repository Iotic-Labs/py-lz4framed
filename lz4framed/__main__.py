# Copyright (c) 2015 Iotic Labs Ltd. All rights reserved.

"""(de)compresses to/from lz4-framed data"""

from __future__ import print_function
from sys import argv, stderr

from .compat import STDIN_RAW, STDOUT_RAW
from . import Compressor, Decompressor, Lz4FramedError, Lz4FramedNoDataError, get_block_size


def __error(*args, **kwargs):
    print(*args, file=stderr, **kwargs)


def do_compress(in_stream, out_stream):
    read = in_stream.read
    read_size = get_block_size()
    try:
        with Compressor(fp=out_stream) as compressor:
            try:
                while True:
                    compressor.update(read(read_size))
            # empty read result supplied to update()
            except Lz4FramedNoDataError:
                pass
            # input stream exception
            except EOFError:
                pass
    except Lz4FramedError as ex:
        __error('Compression error: %s' % ex)
        return 8
    return 0


def do_decompress(in_stream, out_stream):
    write = out_stream.write
    try:
        for chunk in Decompressor(in_stream):
            write(chunk)
    except Lz4FramedError as ex:
        __error('Compression error: %s' % ex)
        return 8
    return 0


__ACTION = frozenset(('compress', 'decompress'))


def main():  # noqa (complexity)
    if not (3 <= len(argv) <= 4 and argv[1] in __ACTION):
        print("""USAGE: lz4framed (compress|decompress) (INFILE|-) [OUTFILE]

(De)compresses an lz4 frame. Input is read from INFILE unless set to '-', in
which case stdin is used. If OUTFILE is not specified, output goes to stdout.""", file=stderr)
        return 1

    compress = (argv[1] == 'compress')
    in_file = out_file = None
    try:
        # input
        if argv[2] == '-':
            in_stream = STDIN_RAW
        else:
            try:
                in_stream = in_file = open(argv[2], 'rb')
            except IOError as ex:
                __error('Failed to open input file for reading: %s' % ex)
                return 2
        # output
        if len(argv) == 3:
            out_stream = STDOUT_RAW
        else:
            try:
                out_stream = out_file = open(argv[2], 'ab')
            except IOError as ex:
                __error('Failed to open output file for appending: %s' % ex)
                return 4

        return (do_compress if compress else do_decompress)(in_stream, out_stream)
    except IOError as ex:
        __error('I/O failure: %s' % ex)
    finally:
        if in_file:
            in_file.close()
        if out_file:
            out_file.close()


if __name__ == "__main__":
    exit(main())
