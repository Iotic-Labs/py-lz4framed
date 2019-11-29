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


"""Note: These tests are not meant to verify all of lz4's behaviour, only the Python functionality"""

from sys import version_info
from unittest import TestCase
from contextlib import contextmanager
from io import BytesIO, SEEK_END

from lz4framed import (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
                       LZ4F_BLOCKSIZE_MAX4MB,
                       LZ4F_COMPRESSION_MAX,
                       LZ4F_ERROR_GENERIC, LZ4F_ERROR_contentChecksum_invalid, LZ4F_ERROR_frameType_unknown,
                       LZ4F_ERROR_srcPtr_wrong, Lz4FramedError, Lz4FramedNoDataError,
                       compress, decompress,
                       create_compression_context, compress_begin, compress_update, compress_end,
                       create_decompression_context, get_frame_info, decompress_update,
                       get_block_size,
                       Compressor, Decompressor)

PY2 = version_info[0] < 3

SHORT_INPUT = b'abcdefghijklmnopqrstuvwxyz0123456789'
LONG_INPUT = SHORT_INPUT * (10**5)
LEVEL_ACCELERATED_MAX = -10


class TestHelperMixin(object):

    def setUp(self):
        super(TestHelperMixin, self).setUp()
        if PY2:
            # avoid deprecation warning
            self.assertRaisesRegex = self.assertRaisesRegexp  # pylint: disable=invalid-name

    def check_compress_short(self, *args, **kwargs):
        self.assertEqual(SHORT_INPUT, decompress(compress(SHORT_INPUT, *args, **kwargs)))

    def check_compress_long(self, *args, **kwargs):
        self.assertEqual(LONG_INPUT, decompress(compress(LONG_INPUT, *args, **kwargs)))

    @contextmanager
    def assertRaisesLz4FramedError(self, code):  # pylint: disable=invalid-name
        try:
            yield
        except Lz4FramedError as ex:
            self.assertEqual(ex.args[1], code, 'Lz4FramedError code mismatch: [%d]: %s' % (ex.args[1], ex.args[0]))
        else:
            self.fail('Lz4FramedError not raised')


class TestCompress(TestHelperMixin, TestCase):

    def test_compress_minimal(self):
        with self.assertRaises(TypeError):
            compress()
        with self.assertRaises(Lz4FramedNoDataError):
            compress(b'')
        self.check_compress_short()

    def test_compress_block_size(self):
        with self.assertRaises(TypeError):
            compress(SHORT_INPUT, block_size_id='1')
        with self.assertRaises(ValueError):
            compress(SHORT_INPUT, block_size_id=-1)
        for block_size in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB,
                           LZ4F_BLOCKSIZE_MAX1MB, LZ4F_BLOCKSIZE_MAX4MB):
            self.check_compress_short(block_size_id=block_size)
            self.check_compress_long(block_size_id=block_size)

    def test_compress_linked_mode(self):
        with self.assertRaises(TypeError):
            compress(SHORT_INPUT, block_mode_linked=None)
        self.check_compress_short(block_mode_linked=True)
        self.check_compress_short(block_mode_linked=False)

    def test_compress_checksum(self):
        with self.assertRaises(TypeError):
            compress(SHORT_INPUT, checksum=None)
        self.check_compress_short(checksum=True)
        self.check_compress_short(checksum=False)
        for data in (SHORT_INPUT, LONG_INPUT):
            with self.assertRaisesLz4FramedError(LZ4F_ERROR_contentChecksum_invalid):
                # invalid checksum
                decompress(compress(data, checksum=True)[:-1] + b'0')

    def test_compress_block_checksum(self):
        with self.assertRaises(TypeError):
            compress(SHORT_INPUT, block_checksum=None)
        self.check_compress_short(block_checksum=True)
        self.check_compress_short(block_checksum=False)

    def test_compress_level(self):
        with self.assertRaises(TypeError):
            compress(SHORT_INPUT, level='1')
        # negative values designate accelerattion
        for level in range(LEVEL_ACCELERATED_MAX, LZ4F_COMPRESSION_MAX + 1):
            self.check_compress_short(level=level)
        # large input, fast & hc levels (levels > 10 (v1.7.5) are significantly slower)
        self.check_compress_long(level=0)
        self.check_compress_long(level=10)

    def test_compress_memoryview(self):
        view = memoryview(LONG_INPUT)
        self.assertEqual(view, decompress(compress(view)))


class TestDecompress(TestHelperMixin, TestCase):

    def test_decompress_minimal(self):
        with self.assertRaises(TypeError):
            decompress()
        with self.assertRaises(Lz4FramedNoDataError):
            decompress(b'')
        self.check_compress_short()

    def test_decompress_buffer_size(self):
        out = compress(SHORT_INPUT)
        with self.assertRaises(TypeError):
            decompress(out, buffer_size='1')
        with self.assertRaises(ValueError):
            decompress(out, buffer_size=0)
        out = compress(LONG_INPUT)
        for buffer_size in range(1, 1025, 128):
            self.assertEqual(LONG_INPUT, decompress(out, buffer_size=buffer_size))

    def test_decompress_invalid_input(self):
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_frameType_unknown):
            decompress(b'invalidheader')
        with self.assertRaisesRegex(ValueError, 'frame incomplete'):
            decompress(compress(SHORT_INPUT)[:-5])
        # incomplete data (length not specified in header)
        with BytesIO() as out:
            with Compressor(out) as compressor:
                compressor.update(SHORT_INPUT)
            output = out.getvalue()
            with self.assertRaisesRegex(ValueError, 'frame incomplete'):
                decompress(output[:-20])

    def test_decompress_memoryview(self):
        view = memoryview(compress(LONG_INPUT))
        self.assertEqual(LONG_INPUT, decompress(view))


class TestLowLevelFunctions(TestHelperMixin, TestCase):

    def test_get_block_size(self):
        with self.assertRaises(TypeError):
            get_block_size('1')
        with self.assertRaises(ValueError):
            get_block_size(1)
        self.assertEqual(get_block_size(), get_block_size(LZ4F_BLOCKSIZE_DEFAULT))
        for size in (LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB, LZ4F_BLOCKSIZE_MAX4MB):
            self.assertEqual(get_block_size(size), 1 << (8 + (2 * size)))

    def test_create_contexts(self):
        for func in (create_compression_context, create_decompression_context):
            self.assertIsNotNone(func())

    def test_get_frame_info(self):
        with self.assertRaises(TypeError):
            get_frame_info()
        with self.assertRaises(ValueError):
            get_frame_info(create_compression_context())

        ctx = create_decompression_context()
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_srcPtr_wrong):
            get_frame_info(ctx)
        # compress with non-default arguments, check info structure
        args = {'checksum': True,
                'block_size_id': LZ4F_BLOCKSIZE_MAX256KB,
                'block_mode_linked': False}
        # Using long input since lz4 adjusts block size is input smaller than one block
        decompress_update(ctx, compress(LONG_INPUT, **args)[:15])
        info = get_frame_info(ctx)
        self.assertTrue(info.pop('input_hint', 0) > 0)
        args['length'] = len(LONG_INPUT)
        self.assertEqual(info, args)

    def __compress_begin(self, **kwargs):
        ctx = create_compression_context()
        header = compress_begin(ctx, **kwargs)
        self.assertTrue(7 <= len(header) <= 15)
        return ctx, header

    def test_compress_begin(self):
        with self.assertRaises(TypeError):
            compress_begin()
        with self.assertRaises(ValueError):
            compress_begin(create_decompression_context())

    def test_compress_begin_block_size(self):
        with self.assertRaises(TypeError):
            self.__compress_begin(block_size_id='1')
        with self.assertRaises(ValueError):
            self.__compress_begin(block_size_id=-1)
        for size in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
                     LZ4F_BLOCKSIZE_MAX4MB):
            self.__compress_begin(block_size_id=size)

    def test_compress_begin_linked_mode(self):
        with self.assertRaises(TypeError):
            self.__compress_begin(block_mode_linked=None)
        self.__compress_begin(block_mode_linked=True)
        self.__compress_begin(block_mode_linked=False)

    def test_compress_begin_checksum(self):
        with self.assertRaises(TypeError):
            self.__compress_begin(checksum=None)
        self.__compress_begin(checksum=True)
        self.__compress_begin(checksum=False)

    def test_compress_begin_level(self):
        with self.assertRaises(TypeError):
            self.__compress_begin(level='1')
        for level in range(LEVEL_ACCELERATED_MAX, LZ4F_COMPRESSION_MAX + 1):
            self.__compress_begin(level=level)

    def test_compress_update_invalid(self):
        with self.assertRaises(TypeError):
            compress_update()
        with self.assertRaises(TypeError):
            compress_update(1)
        # invalid context
        with self.assertRaises(ValueError):
            compress_update(create_decompression_context(), b' ')
        # data before compress_begin called
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_GENERIC):
            compress_update(create_compression_context(), b' ')

        ctx, _ = self.__compress_begin()
        # invalid data
        with self.assertRaises(TypeError):
            compress_update(ctx, 1)
        # empty data
        with self.assertRaises(Lz4FramedNoDataError):
            compress_update(ctx, b'')

    def test_compress_end(self):
        with self.assertRaises(TypeError):
            compress_end()
        with self.assertRaises(ValueError):
            compress_end(create_decompression_context())

        ctx, header = self.__compress_begin()
        self.assertEqual(b'', decompress(header + compress_end(ctx)))

        ctx, header = self.__compress_begin()
        data = compress_update(ctx, SHORT_INPUT)
        self.assertEqual(decompress(header + data + compress_end(ctx)), SHORT_INPUT)

    def __compress_with_data_and_args(self, data, **kwargs):
        ctx, header = self.__compress_begin(**kwargs)
        in_raw = BytesIO(data)
        out = BytesIO(header)
        out.seek(0, SEEK_END)
        try:
            while True:
                out.write(compress_update(ctx, in_raw.read(1024)))
        except Lz4FramedNoDataError:
            pass
        out.write(compress_end(ctx))
        self.assertEqual(decompress(out.getvalue()), data)

    def test_compress(self):
        func = self.__compress_with_data_and_args

        for size in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
                     LZ4F_BLOCKSIZE_MAX4MB):
            func(LONG_INPUT, block_size_id=size)

        for arg in ('block_mode_linked', 'checksum'):
            for value in (False, True):
                func(LONG_INPUT, **{arg: value})

        for level in range(LEVEL_ACCELERATED_MAX, LZ4F_COMPRESSION_MAX + 1):
            func(SHORT_INPUT, level=level)

        func(memoryview(LONG_INPUT))

    def test_decompress_update_invalid(self):
        with self.assertRaises(TypeError):
            decompress_update()
        with self.assertRaises(TypeError):
            decompress_update(1)
        # invalid context
        with self.assertRaises(ValueError):
            decompress_update(create_compression_context(), b' ')

        ctx = create_decompression_context()

        with self.assertRaises(TypeError):
            decompress_update(ctx, b' ', chunk_len='1')
        with self.assertRaises(ValueError):
            decompress_update(ctx, b' ', chunk_len=0)

        in_raw = compress(LONG_INPUT, checksum=True)

        ret = decompress_update(ctx, in_raw[:512], chunk_len=2)
        # input_hint
        self.assertTrue(ret.pop() > 0)
        # chunk length
        self.assertTrue(len(ret) > 0)
        self.assertTrue(all(1 <= len(chunk) <= 2 for chunk in ret))

        # invalid input (from start of frame)
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_GENERIC):
            decompress_update(ctx, in_raw)

        # checksum invalid
        in_raw = in_raw[:-4] + b'1234'
        ctx = create_decompression_context()
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_contentChecksum_invalid):
            decompress_update(ctx, in_raw)

    def test_decompress_update_memoryview(self):  # pylint: disable=invalid-name
        ctx = create_decompression_context()
        data = decompress_update(ctx, memoryview(compress(LONG_INPUT)))
        self.assertEqual(b''.join(data[:-1]), LONG_INPUT)


class TestCompressor(TestHelperMixin, TestCase):
    """Note: Low-level methods supporting Compressor class have been tested in TestLowLevelFunctions"""

    def test_compressor_init(self):
        with self.assertRaisesRegex(AttributeError, 'has no attribute \'write\''):
            Compressor('1')

        # non-callable write attribute
        class Empty(object):
            write = 1
        with self.assertRaises(TypeError):
            Compressor(Empty())

        # cannot use context without fp
        with self.assertRaises(ValueError):
            with Compressor() as _:  # noqa (unused variable)
                pass

    def test_compressor__no_fp(self):
        in_bytes = BytesIO(LONG_INPUT)
        out_bytes = BytesIO()

        compressor = Compressor()
        try:
            while True:
                out_bytes.write(compressor.update(in_bytes.read(1024)))
        # raised by compressor.update() on empty data argument
        except Lz4FramedNoDataError:
            pass
        out_bytes.write(compressor.end())

        self.assertEqual(decompress(out_bytes.getvalue()), LONG_INPUT)

    def test_compressor_fp(self):
        self.__fp_test()

    def __fp_test(self, in_raw=LONG_INPUT, **kwargs):
        in_bytes = BytesIO(in_raw)
        out_bytes = BytesIO()

        with Compressor(out_bytes, **kwargs) as compressor:
            try:
                while True:
                    compressor.update(in_bytes.read(1024))
            # raised by compressor.update() on empty data argument
            except Lz4FramedNoDataError:
                pass
        self.assertEqual(decompress(out_bytes.getvalue()), in_raw)

    def test_compressor_block_size(self):
        for block_size in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB,
                           LZ4F_BLOCKSIZE_MAX1MB, LZ4F_BLOCKSIZE_MAX4MB):
            self.__fp_test(block_size_id=block_size)

    def test_compressor_checksum(self):
        self.__fp_test(checksum=False)
        self.__fp_test(checksum=True)

    def test_compressor_autoflush(self):
        self.__fp_test(autoflush=True)
        self.__fp_test(autoflush=False)

    def test_compressor_level(self):
        for level in range(LEVEL_ACCELERATED_MAX, LZ4F_COMPRESSION_MAX + 1):
            self.__fp_test(in_raw=SHORT_INPUT, level=level)
        self.__fp_test(level=0)
        # levels > 10 (v1.7.5) are significantly slower
        self.__fp_test(level=10)


class TestDecompressor(TestHelperMixin, TestCase):

    def test_decompressor_init(self):
        with self.assertRaises(TypeError):
            Decompressor()  # pylint: disable=no-value-for-parameter
        with self.assertRaisesRegex(AttributeError, 'has no attribute \'read\''):
            Decompressor('1')

        # non-callable read attribute
        class Empty(object):
            read = 1
        with self.assertRaises(TypeError):
            Decompressor(Empty())

    def test_decompressor_fp(self):
        # levels > 10 (v1.7.5) are significantly slower
        for level in (LEVEL_ACCELERATED_MAX, 10):
            out_bytes = BytesIO()
            for chunk in Decompressor(BytesIO(compress(LONG_INPUT, level=level))):
                out_bytes.write(chunk)
            self.assertEqual(out_bytes.getvalue(), LONG_INPUT)

        # incomplete frame
        out_bytes.truncate()
        with self.assertRaises(Lz4FramedNoDataError):
            for chunk in Decompressor(BytesIO(compress(LONG_INPUT)[:-32])):
                out_bytes.write(chunk)
        # some data should have been written
        out_bytes.seek(SEEK_END)
        self.assertTrue(out_bytes.tell() > 0)


# def pympler_run(iterations=20):
#     from unittest import main
#     from pympler import tracker
#     from gc import collect

#     tracker = tracker.SummaryTracker()
#     for i in range(iterations):
#         try:
#             main()
#         except SystemExit:
#             pass
#         if i % 2:
#             collect()
#             tracker.print_diff()


# if __name__ == '__main__':
#     pympler_run()
