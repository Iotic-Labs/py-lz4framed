# Copyright (c) 2015 Iotic Labs Ltd. All rights reserved.

"""Note: These tests are not meant to verify all of lz4's behaviour, only the Python functionality"""

from sys import version_info
from unittest import TestCase
from contextlib import contextmanager
from io import BytesIO, SEEK_END

from lz4framed import (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
                       LZ4F_BLOCKSIZE_MAX4MB,
                       LZ4F_ERROR_GENERIC, LZ4F_ERROR_frameHeader_incomplete, LZ4F_ERROR_contentChecksum_invalid,
                       Lz4FramedError, Lz4FramedNoDataError,
                       compress, decompress,
                       create_compression_context, compress_begin, compress_update, compress_end,
                       create_decompression_context, get_frame_info, decompress_update,
                       get_block_size,
                       Compressor, Decompressor)

PY2 = version_info[0] < 3

_shortInput = b'abcdefghijklmnopqrstuvwxyz0123456789'
_longInput = _shortInput * (10**5)


class TestHelperMixin(object):

    def setUp(self):
        super(TestHelperMixin, self).setUp()
        if PY2:
            # avoid deprecation warning
            self.assertRaisesRegex = self.assertRaisesRegexp

    def checkCompressShort(self, *args, **kwargs):
        self.assertEqual(_shortInput, decompress(compress(_shortInput, *args, **kwargs)))

    def checkCompressLong(self, *args, **kwargs):
        self.assertEqual(_longInput, decompress(compress(_longInput, *args, **kwargs)))

    @contextmanager
    def assertRaisesLz4FramedError(self, code):
        try:
            yield
        except Lz4FramedError as e:
            self.assertEqual(e.args[1], code, 'Lz4FramedError code mismatch: [%d]: %s' % (e.args[1], e.args[0]))
        else:
            self.fail('Lz4FramedError not raised')


class TestCompress(TestHelperMixin, TestCase):

    def test_compress_minimal(self):
        with self.assertRaises(TypeError):
            compress()
        with self.assertRaises(Lz4FramedNoDataError):
            compress(b'')
        self.checkCompressShort()

    def test_compress_block_size(self):
        with self.assertRaises(TypeError):
            compress(_shortInput, block_size_id='1')
        with self.assertRaises(ValueError):
            compress(_shortInput, block_size_id=-1)
        for blockSize in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB,
                          LZ4F_BLOCKSIZE_MAX1MB, LZ4F_BLOCKSIZE_MAX4MB):
            self.checkCompressShort(block_size_id=blockSize)
            self.checkCompressLong(block_size_id=blockSize)

    def test_compress_linked_mode(self):
        with self.assertRaises(TypeError):
            compress(_shortInput, block_mode_linked=None)
        self.checkCompressShort(block_mode_linked=True)
        self.checkCompressShort(block_mode_linked=False)

    def test_compress_checksum(self):
        with self.assertRaises(TypeError):
            compress(_shortInput, checksum=None)
        self.checkCompressShort(checksum=True)
        self.checkCompressShort(checksum=False)
        for data in (_shortInput, _longInput):
            with self.assertRaisesLz4FramedError(LZ4F_ERROR_contentChecksum_invalid):
                # invalid checksum
                decompress(compress(data, checksum=True)[:-1] + b'0')

    def test_compress_level(self):
        with self.assertRaises(TypeError):
            compress(_shortInput, level='1')
        with self.assertRaises(ValueError):
            compress(_shortInput, level=-1)
        for level in range(17):
            self.checkCompressShort(level=level)
        # large input, fast & hc levels
        self.checkCompressLong(level=0)
        self.checkCompressLong(level=16)


class TestDecompress(TestHelperMixin, TestCase):

    def test_decompress_minimal(self):
        with self.assertRaises(TypeError):
            decompress()
        with self.assertRaises(Lz4FramedNoDataError):
            decompress(b'')
        self.checkCompressShort()

    def test_decompress_buffer_size(self):
        out = compress(_shortInput)
        with self.assertRaises(TypeError):
            decompress(out, buffer_size='1')
        with self.assertRaises(ValueError):
            decompress(out, buffer_size=0)
        out = compress(_longInput)
        for buffer_size in range(1, 1025, 128):
            self.assertEqual(_longInput, decompress(out, buffer_size=buffer_size))

    def test_decompress_invalid_input(self):
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_frameHeader_incomplete):
            decompress(b'invalidheader')
        with self.assertRaisesRegex(ValueError, 'frame incomplete'):
            decompress(compress(_shortInput)[:-5])


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
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_frameHeader_incomplete):
            get_frame_info(ctx)
        # compress with non-default arguments, check info structure
        args = {'checksum': True,
                'block_size_id': LZ4F_BLOCKSIZE_MAX256KB,
                'block_mode_linked': False}
        # Using long input since lz4 adjusts block size is input smaller than one block
        decompress_update(ctx, compress(_longInput, **args)[:15])
        info = get_frame_info(ctx)
        self.assertTrue(info.pop('input_hint', 0) > 0)
        args['length'] = len(_longInput)
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
        with self.assertRaises(ValueError):
            self.__compress_begin(level=-1)
        for level in range(17):
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
        # without any compress_update calls frame is invalid
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_frameHeader_incomplete):
            decompress(header + compress_end(ctx))

        ctx, header = self.__compress_begin()
        data = compress_update(ctx, _shortInput)
        self.assertEqual(decompress(header + data + compress_end(ctx)), _shortInput)

    def __compress_with_data_and_args(self, data, **kwargs):
        ctx, header = self.__compress_begin(**kwargs)
        inRaw = BytesIO(data)
        out = BytesIO(header)
        out.seek(0, SEEK_END)
        try:
            while True:
                out.write(compress_update(ctx, inRaw.read(1024)))
        except Lz4FramedNoDataError:
            pass
        out.write(compress_end(ctx))
        self.assertEqual(decompress(out.getvalue()), data)

    def test_compress(self):
        func = self.__compress_with_data_and_args

        for size in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB, LZ4F_BLOCKSIZE_MAX1MB,
                     LZ4F_BLOCKSIZE_MAX4MB):
            func(_longInput, block_size_id=size)

        for arg in ('block_mode_linked', 'checksum'):
            for value in (False, True):
                func(_longInput, **{arg: value})

        for level in range(17):
            func(_shortInput, level=level)
        func(_shortInput, level=0)
        func(_shortInput, level=16)

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

        inRaw = compress(_longInput, checksum=True)

        ret = decompress_update(ctx, inRaw[:512], chunk_len=2)
        # input_hint
        self.assertTrue(ret.pop() > 0)
        # chunk length
        self.assertTrue(len(ret) > 0)
        self.assertTrue(all(1 <= len(chunk) <= 2 for chunk in ret))

        # invalid input (from start of frame)
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_GENERIC):
            decompress_update(ctx, inRaw)

        # checksum invalid
        inRaw = inRaw[:-4] + b'1234'
        ctx = create_decompression_context()
        with self.assertRaisesLz4FramedError(LZ4F_ERROR_contentChecksum_invalid):
            decompress_update(ctx, inRaw)


class TestCompressor(TestHelperMixin, TestCase):
    """Note: Low-level methods supporting Compressor class have been tested in TestLowLevelFunctions"""

    def test_compressor_init(self):
        with self.assertRaisesRegex(AttributeError, 'has no attribute \'write\''):
            Compressor(fp='1')

        # non-callable write attribute
        class Empty(object):
            write = 1
        with self.assertRaises(TypeError):
            Compressor(fp=Empty())

        # cannot use context without fp
        with self.assertRaises(ValueError):
            with Compressor() as _:  # noqa (unused variable)
                pass

    def test_compressor__no_fp(self):
        inB = BytesIO(_longInput)
        outB = BytesIO()

        c = Compressor()
        try:
            while True:
                outB.write(c.update(inB.read(1024)))
        # raised by compressor.update() on empty data argument
        except Lz4FramedNoDataError:
            pass
        outB.write(c.end())

        self.assertEqual(decompress(outB.getvalue()), _longInput)

    def test_compressor_fp(self):
        self.__fp_test()

    def __fp_test(self, inRaw=_longInput, **kwargs):
        inB = BytesIO(inRaw)
        outB = BytesIO()

        with Compressor(fp=outB, **kwargs) as c:
            try:
                while True:
                    c.update(inB.read(1024))
            # raised by compressor.update() on empty data argument
            except Lz4FramedNoDataError:
                pass
        self.assertEqual(decompress(outB.getvalue()), inRaw)

    def test_compressor_block_size(self):
        for blockSize in (LZ4F_BLOCKSIZE_DEFAULT, LZ4F_BLOCKSIZE_MAX64KB, LZ4F_BLOCKSIZE_MAX256KB,
                          LZ4F_BLOCKSIZE_MAX1MB, LZ4F_BLOCKSIZE_MAX4MB):
            self.__fp_test(block_size_id=blockSize)

    def test_compressor_checksum(self):
        self.__fp_test(checksum=False)
        self.__fp_test(checksum=True)

    def test_compressor_autoflush(self):
        self.__fp_test(autoflush=True)
        self.__fp_test(autoflush=False)

    def test_compressor_level(self):
        for level in range(17):
            self.__fp_test(inRaw=_shortInput, level=level)
        self.__fp_test(level=0)
        self.__fp_test(level=16)


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
        for level in (0, 16):
            outB = BytesIO()
            for chunk in Decompressor(BytesIO(compress(_longInput, level=level))):
                outB.write(chunk)
            self.assertEqual(outB.getvalue(), _longInput)

        # incomplete frame
        outB.truncate()
        with self.assertRaises(Lz4FramedNoDataError):
            for chunk in Decompressor(BytesIO(compress(_longInput)[:-32])):
                outB.write(chunk)
        # some data should have been written
        outB.seek(SEEK_END)
        self.assertTrue(outB.tell() > 0)
