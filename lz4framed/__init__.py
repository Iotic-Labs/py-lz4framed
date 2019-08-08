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


"""lz4 frame compression library, bound to lz4 C implementation

Example usage:

# To compress
compressed = lz4framed.compress(b'binary data')

# To decode
uncompressed = lz4framed.decompress(compressed)

To use a file-like objects as input/output, use the provided Compressor & Decompressor
classes instead or manually utilise the context-using low-level methods. All methods are thread safe unless stated.
"""

from threading import Lock

from _lz4framed import (  # noqa (unused import)
    LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
    LZ4F_BLOCKSIZE_MAX4MB,
    LZ4F_COMPRESSION_MIN, LZ4F_COMPRESSION_MIN_HC, LZ4F_COMPRESSION_MAX,
    LZ4F_ERROR_GENERIC, LZ4F_ERROR_maxBlockSize_invalid, LZ4F_ERROR_blockMode_invalid,
    LZ4F_ERROR_contentChecksumFlag_invalid, LZ4F_ERROR_compressionLevel_invalid, LZ4F_ERROR_headerVersion_wrong,
    LZ4F_ERROR_blockChecksum_invalid, LZ4F_ERROR_reservedFlag_set, LZ4F_ERROR_allocation_failed,
    LZ4F_ERROR_srcSize_tooLarge, LZ4F_ERROR_dstMaxSize_tooSmall, LZ4F_ERROR_frameHeader_incomplete,
    LZ4F_ERROR_frameType_unknown, LZ4F_ERROR_frameSize_wrong, LZ4F_ERROR_srcPtr_wrong, LZ4F_ERROR_decompressionFailed,
    LZ4F_ERROR_headerChecksum_invalid, LZ4F_ERROR_contentChecksum_invalid, LZ4F_ERROR_frameDecoding_alreadyStarted,
    LZ4F_VERSION, LZ4_VERSION, __version__,
    Lz4FramedError, Lz4FramedNoDataError,
    compress, decompress,
    create_compression_context, compress_begin, compress_update, compress_end,
    create_decompression_context, get_frame_info, decompress_update,
    get_block_size
)

from .compat import Iterable as __Iterable


class Compressor(object):
    """Iteratively compress data in lz4-framed - can be used as a context manager if writing to a file, e.g.:

        with open('myFile', 'wb') as f:
            # Context automatically finalises frame on completion, unless an exception occurs
            with Compressor(f) as c:
                try:
                    while (...):
                       c.update(moreData)
                except Lz4FramedNoDataError:
                    pass

    Alternatively, with output from relevant methods:

        c = Compressor()
        while (...):
            try:
                someOutput.append(c.update(moreData))
            except Lz4FramedNoDataError:
                pass
        # Finalise frame
        someOutput.append(c.end())
    """

    def __init__(self, fp=None, block_size_id=LZ4F_BLOCKSIZE_DEFAULT, block_mode_linked=True, checksum=False,
                 autoflush=False, level=LZ4F_COMPRESSION_MIN, block_checksum=False):
        """
        Args:
            fp: File like object (supporting write() method) to write compressed data to. If not set, data will be
                returned by the update(), flush() and end() methods.
            block_size_id (int): Compression block size identifier. One of the LZ4F_BLOCKSIZE_* constants
            block_mode_linked (bool): Whether compression blocks are linked
            checksum (bool): Whether to produce frame checksum
            autoflush (bool): Whether to return (or write to fp) compressed data on each update() call rather than
                              waiting for internal buffer to be filled. (This reduces internal buffer size.)
            level (int): Compression level. Values lower than 3 (including negative ones) use fast compression.
                         Recommended range for hc compression is between 4 and 9, with a maximum of LZ4_COMPRESSION_MAX.
            block_checksum (bool): Whether to produce checksum after each block
        """
        self.__ctx = create_compression_context()
        self.__lock = Lock()
        if fp is None:
            self.__write = None
        elif not callable(fp.write):
            raise TypeError('fp.write not callable')
        else:
            self.__write = fp.write
        self.__header = compress_begin(self.__ctx, block_size_id=block_size_id, block_mode_linked=block_mode_linked,
                                       checksum=checksum, autoflush=autoflush, level=level,
                                       block_checksum=block_checksum)

    def __enter__(self):
        if self.__write is None:
            raise ValueError('Context only usable when fp supplied')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end()

    def update(self, b):  # pylint: disable=method-hidden,invalid-name
        """Compress data given in b, returning compressed result either from this function or writing to fp). Note:
           sometimes output might be zero length (if being buffered by lz4).
           Raises Lz4FramedNoDataError if input is of zero length."""
        with self.__lock:
            output = compress_update(self.__ctx, b)
            if self.__write:
                self.__write(self.__header)
                self.__header = None
                self.__write(output)
                self.update = self.__updateNextWrite
                return None

            header = self.__header
            self.__header = None
            self.update = self.__updateNextReturn
            return header + output

    # post-first update methods so do not require header write & fp checks
    def __updateNextWrite(self, b):  # pylint: disable=invalid-name
        self.__write(compress_update(self.__ctx, b))

    def __updateNextReturn(self, b):  # pylint: disable=invalid-name
        return compress_update(self.__ctx, b)

    def end(self):
        """Finalise lz4 frame, outputting any remaining as return from this function or by writing to fp)"""
        with self.__lock:
            if self.__write:
                self.__write(compress_end(self.__ctx))
                return None

            return compress_end(self.__ctx)


class Decompressor(__Iterable):  # pylint: disable=super-init-not-called
    """Iteratively decompress blocks of an lz4-frame from a file-like object, e.g.:

        with open('myFile', 'rb') as f:
            try:
                for chunk in Decompressor(f):
                   decoded.append(chunk)
            except Lz4FramedNoDataError:
                # Frame incomplete - error case

    The decompressor will automatically choose a meaningful read size. Note that some
    iterator calls might return zero-length data. The iterator raises LZ4FNoDataError
    if input (from fp.read) is of zero length, before decompression finished.
    """

    def __init__(self, fp):
        """
        Args:
            fp: File like object (supporting read() method) to read compressed data from.
        """
        super(Decompressor, self).__init__()
        if fp is None:
            raise TypeError('fp')
        elif not callable(fp.read):
            raise TypeError('fp.read not callable')
        else:
            self.__read = fp.read
        self.__info = None
        self.__ctx = create_decompression_context()
        self.__lock = Lock()

    def __iter__(self):
        ctx = self.__ctx
        read = self.__read
        input_hint = 15  # enough to read largest header
        chunk_size = 32  # output chunk size, will be increased once block size known

        with self.__lock:
            output = decompress_update(ctx, read(input_hint), chunk_size)
            try:
                self.__info = info = get_frame_info(ctx)
            except Lz4FramedError as ex:
                if ex.args[1] != LZ4F_ERROR_frameHeader_incomplete:
                    # should not happen since have read 15 bytes
                    raise
            else:
                chunk_size = get_block_size(info['block_size_id'])
            input_hint = output.pop()

            # return any data as part of header read, if present
            for element in output:
                yield element

            while input_hint > 0:
                output = decompress_update(ctx, read(input_hint), chunk_size)
                input_hint = output.pop()
                for element in output:
                    yield element

    @property
    def frame_info(self):
        """See get_frame_info(). Note: This will return None if not enough data has been
           read yet to decode header (typically at least one read from iterator)."""
        return self.__info
