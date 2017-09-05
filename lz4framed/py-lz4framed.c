/*
 * Copyright (c) 2016 Iotic Labs Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://github.com/Iotic-Labs/py-lz4framed/blob/master/LICENSE
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// byte/string argument parsing size as Py_ssize_t (e.g. via PyArg_ParseTupleAndKeywords)
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <bytesobject.h>

#include "lz4frame_static.h"
#include "lz4hc.h"

/******************************************************************************/

#define UNUSED(x) (void)(x)
#define QUOTE(str) #str
#define EXPAND_AND_QUOTE(str) QUOTE(str)
#define MAX(x, y) (x) >= (y) ? (x) : (y)
#define KB *(1<<10)
#define MB *(1<<20)
#define LZ4_COMPRESSION_MIN 0
#define LZ4_COMPRESSION_MIN_HC LZ4HC_CLEVEL_MIN
#define LZ4_COMPRESSION_MAX LZ4HC_CLEVEL_MAX


#define _BAIL_ON_LZ4_ERROR(code, without_gil) {\
    size_t __err;\
    if (without_gil) {\
        Py_BEGIN_ALLOW_THREADS;\
        __err = (code);\
        Py_END_ALLOW_THREADS;\
    } else {\
        __err = (code);\
    }\
    if (LZ4F_isError(__err)) {\
        PyObject *num = NULL, *str = NULL, *tuple = NULL;\
        if ((num = PyLong_FromSize_t(-(int)__err)) &&\
            (str = PyUnicode_FromString(LZ4F_getErrorName(__err))) &&\
            (tuple = PyTuple_Pack(2, str, num))) {\
            PyErr_SetObject(LZ4FError, tuple);\
        /* backup method in case object creation fails */\
        } else {\
            PyErr_Format(LZ4FError, "[%d] %s", -(int)__err, LZ4F_getErrorName(__err));\
        }\
        Py_XDECREF(tuple);\
        Py_XDECREF(num);\
        Py_XDECREF(str);\
        goto bail;\
    }\
}
#define BAIL_ON_LZ4_ERROR(code) _BAIL_ON_LZ4_ERROR((code), 0)

#ifdef WITH_THREAD
    #include <pythread.h>
    #define LZ4FRAMED_LOCK_FLAG int lock_acquired = 0
    #define ENTER_LZ4FRAMED(ctx) \
        if (!lock_acquired) {\
            Py_BEGIN_ALLOW_THREADS;\
            PyThread_acquire_lock((ctx)->lock, 1);\
            Py_END_ALLOW_THREADS;\
            lock_acquired = 1;\
        }
    #define EXIT_LZ4FRAMED(ctx) \
        if (NULL != (ctx) && lock_acquired) {\
            PyThread_release_lock((ctx)->lock);\
            lock_acquired = 0;\
        }
    #define BAIL_ON_LZ4_ERROR_NOGIL(code) _BAIL_ON_LZ4_ERROR((code), 1)
#else
    #define LZ4FRAMED_LOCK_FLAG
    #define ENTER_LZ4FRAMED(ctx)
    #define EXIT_LZ4FRAMED(ctx)
    #define BAIL_ON_LZ4_ERROR_NOGIL(code) BAIL_ON_LZ4_ERROR(code)
#endif
// How large buffers have to be at least to release GIL
#define NOGIL_COMPRESS_INPUT_SIZE_THRESHOLD 8*1024
#define NOGIL_DECOMPRESS_INPUT_SIZE_THRESHOLD 8*1024
#define NOGIL_DECOMPRESS_OUTPUT_SIZE_THRESHOLD 8*1024



#define BAIL_ON_NULL(result) \
if (NULL == (result)) {\
    goto bail;\
}

#define BAIL_ON_NONZERO(result) \
if (result) {\
    goto bail;\
}

#define COMPRESSION_CAPSULE_NAME "_lz4fcctx"
#define DECOMPRESSION_CAPSULE_NAME "_lz4fdctx"

PyDoc_STRVAR(__lz4f_error__doc__,
             "Raised when an lz4-specific error occurs. Arguments are the error message and associated code.");
static PyObject *LZ4FError = NULL;
PyDoc_STRVAR(__lz4f_no_data_error__doc__,
             "Raised by compress_update() and compress() when data supplied is of zero length");
static PyObject *LZ4FNoDataError = NULL;

/* Hold compression context together with preferences, so compress_update & compress_end can calculate right output size
 * based on actualy preferences previously set via compress_begin (rather than defaults). The lock is used to preserve
 * thread safety when releasing GIL.
 */
typedef struct {
    LZ4F_compressionContext_t ctx;
    LZ4F_preferences_t prefs;
#ifdef WITH_THREAD
    PyThread_type_lock lock;
#endif
} _lz4f_cctx_t;

typedef struct {
    LZ4F_decompressionContext_t ctx;
#ifdef WITH_THREAD
    PyThread_type_lock lock;
#endif
} _lz4f_dctx_t;

static LZ4F_preferences_t prefs_defaults = {{0, 0, 0, 0, 0, 0, 0}, 0, 0, {0}};

/******************************************************************************/

static int _valid_lz4f_block_size_id(int id) {
    switch (id) {
        case LZ4F_default:
        case LZ4F_max64KB:
        case LZ4F_max256KB:
        case LZ4F_max1MB:
        case LZ4F_max4MB:
            return 1;
        default:
            return 0;
    }
}

static size_t _lz4f_block_size_from_id(int id) {
    static const size_t blockSizes[4] = { 64 KB, 256 KB, 1 MB, 4 MB };

    if (!_valid_lz4f_block_size_id(id)) {
        return 0;
    }
    if (id == LZ4F_default) {
        id = LZ4F_max64KB;
    }
    id -= 4;
    return blockSizes[id];
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_get_block_size__doc__,
"get_block_size(id=LZ4F_BLOCKSIZE_DEFAULT) -> int\n"
"\n"
"Returns block size in bytes for the given lz4 block size id\n"
"\n"
"Args:\n"
"    id (int): One of LZ4F_BLOCKSIZE_* constants, e.g. retrieved via get_frame_info()\n");
#define FUNC_DEF_GET_BLOCK_SIZE {"get_block_size", (PyCFunction)_lz4framed_get_block_size, METH_VARARGS,\
                                 _lz4framed_get_block_size__doc__}
static PyObject*
_lz4framed_get_block_size(PyObject *self, PyObject *args) {
    int block_id = LZ4F_default;
    PyObject *byte_count = NULL;
    UNUSED(self);

    if (!PyArg_ParseTuple(args, "|i:get_block_size", &block_id)) {
        goto bail;
    }
    if (!_valid_lz4f_block_size_id(block_id)) {
        PyErr_Format(PyExc_ValueError, "id (%d) invalid", block_id);
        goto bail;
    }
    BAIL_ON_NULL(byte_count = PyLong_FromSize_t(_lz4f_block_size_from_id(block_id)));

    return byte_count;

bail:
    Py_XDECREF(byte_count);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_compress__doc__,
"compress(b, block_size_id=LZ4F_BLOCKSIZE_DEFAULT, block_mode_linked=True,\n"
"         checksum=False, level=0, block_checksum=False) -> bytes\n"
"\n"
"Compresses the data given in b, returning the compressed and lz4-framed\n"
"result.\n"
"\n"
"Args:\n"
"    b (bytes-like object): The object containing data to compress\n"
"    block_size_id (int): Compression block size identifier, one of the\n"
"                         LZ4F_BLOCKSIZE_* constants\n"
"    block_mode_linked (bool): Whether compression blocks are linked. Better compression\n"
"                              is achieved in linked mode.\n"
"    checksum (bool): Whether to produce frame checksum\n"
"    level (int): Compression level. Values lower than LZ4F_COMPRESSION_MIN_HC use fast\n"
"                 compression. Recommended range for hc compression is between 4 and 9,\n"
"                 with a maximum of LZ4F_COMPRESSION_MAX.\n"
"    block_checksum (bool): Whether to produce checksum after each block.\n"
"\n"
"Raises:\n"
"    LZ4FNoDataError: If provided data is of zero length. (Useful for ending compression loop.)\n"
"    Lz4FramedError: If a compression failure occured");
#define FUNC_DEF_COMPRESS {"compress", (PyCFunction)_lz4framed_compress, METH_VARARGS | METH_KEYWORDS,\
                           _lz4framed_compress__doc__}
static PyObject*
_lz4framed_compress(PyObject *self, PyObject *args, PyObject *kwargs) {
#if PY_MAJOR_VERSION >= 3
    static const char *format = "y*|iiiii:compress";
#else
    static const char *format = "s*|iiiii:compress";
#endif
    static char *keywords[] = {"b", "block_size_id", "block_mode_linked", "checksum", "level", "block_checksum", NULL};

    LZ4F_preferences_t prefs = prefs_defaults;
    Py_buffer input;
    int input_held = 0; // whether Py_buffer (input) needs to be released
    int block_id = LZ4F_default;
    int block_mode_linked = 1;
    int block_checksum = 0;
    int checksum = 0;
    int compression_level = LZ4_COMPRESSION_MIN;
    PyObject *output = NULL;
    char * output_str;
    size_t output_len;
    UNUSED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, format, keywords, &input, &block_id, &block_mode_linked, &checksum,
                                     &compression_level, &block_checksum)) {
        goto bail;
    }
    input_held = 1;

    if (!PyBuffer_IsContiguous(&input, 'C')) {
        PyErr_SetString(PyExc_ValueError, "input not contiguous");
        goto bail;
    }
    if (input.len <= 0) {
        PyErr_SetNone(LZ4FNoDataError);
        goto bail;
    }
    if (!_valid_lz4f_block_size_id(block_id)) {
        PyErr_Format(PyExc_ValueError, "block_size_id (%d) invalid", block_id);
        goto bail;
    }
    if (compression_level > LZ4_COMPRESSION_MAX) {
        PyErr_Format(PyExc_ValueError, "level (%d) invalid", compression_level);
        goto bail;
    }

    prefs.frameInfo.contentSize = input.len;
    prefs.frameInfo.blockMode = block_mode_linked ? LZ4F_blockLinked : LZ4F_blockIndependent;
    prefs.frameInfo.blockSizeID = block_id;
    prefs.frameInfo.blockChecksumFlag = block_checksum ? LZ4F_blockChecksumEnabled : LZ4F_noBlockChecksum;
    prefs.frameInfo.contentChecksumFlag = checksum ? LZ4F_contentChecksumEnabled : LZ4F_noContentChecksum;
    prefs.compressionLevel = compression_level;

    BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressFrameBound(input.len, &prefs));
    BAIL_ON_NULL(output = PyBytes_FromStringAndSize(NULL, output_len));
    BAIL_ON_NULL(output_str = PyBytes_AsString(output));

    if (input.len < NOGIL_COMPRESS_INPUT_SIZE_THRESHOLD) {
        BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressFrame(output_str, output_len, input.buf, input.len, &prefs));
    } else {
        BAIL_ON_LZ4_ERROR_NOGIL(output_len = LZ4F_compressFrame(output_str, output_len, input.buf, input.len, &prefs));
    }
    // output length might be shorter than estimated
    BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));

    PyBuffer_Release(&input);
    input_held = 0;
    return output;

bail:
    if (input_held) {
        PyBuffer_Release(&input);
    }
    Py_XDECREF(output);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_decompress__doc__,
"decompress(b, buffer_size=1024) -> bytes\n"
"\n"
"Decompresses framed lz4 blocks from the data given in *b*, returning the\n"
"uncompressed result. For large payloads consider using Decompressor class\n"
"to decompress in chunks.\n"
"\n"
"Args:\n"
"    b (bytes-like object): The object containing lz4-framed data to decompress\n"
"    buffer_size (int): Initial size of buffer in bytes for decompressed\n"
"                       result. This is useful if the frame is not expected\n"
"                       to indicate uncompressed length of data. If\n"
"                       buffer_size is not large enough, it will be doubled\n"
"                       until the resulting data fits. If the frame states\n"
"                       uncompressed size or if len(b) > buffer_size, this\n"
"                       parameter is ignored.\n"
"\n"
"Raises:\n"
"    LZ4FNoDataError: If provided data is of zero length\n"
"    Lz4FramedError: If a decompression failure occured");
#define FUNC_DEF_DECOMPRESS {"decompress", (PyCFunction)_lz4framed_decompress, METH_VARARGS | METH_KEYWORDS,\
                             _lz4framed_decompress__doc__}
static PyObject*
_lz4framed_decompress(PyObject *self, PyObject *args, PyObject *kwargs) {
#if PY_MAJOR_VERSION >= 3
    static const char *format = "y*|i:decompress";
#else
    static const char *format = "s*|i:decompress";
#endif
    static char *keywords[] = {"b", "buffer_size", NULL};

    LZ4F_decompressionContext_t ctx = NULL;
    LZ4F_decompressOptions_t opt = {0, {0}};
    LZ4F_frameInfo_t frame_info;
    Py_buffer input;
    int input_held = 0;             // whether Py_buffer (input) needs to be released
    const char *input_pos;          // position in input
    size_t input_remaining;         // bytes remaining in input
    size_t input_read;              // used by LZ4 functions to indicate how many bytes were / can be read
    size_t input_size_hint;         // LZ4 hint to how many bytes make up the remaining block + next header
    int buffer_size = 1024;
    PyObject *output = NULL;
    char *output_pos;               // position in output
    size_t output_len;              // size of output
    size_t output_remaining;        // bytes still available in output
    size_t output_written;          // used by LZ4 to indicate how many bytes were / can be written
    UNUSED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, format, keywords, &input, &buffer_size)) {
        goto bail;
    }
    input_held = 1;

    if (!PyBuffer_IsContiguous(&input, 'C')) {
        PyErr_SetString(PyExc_ValueError, "input not contiguous");
        goto bail;
    }
    if (input.len <= 0) {
        PyErr_SetNone(LZ4FNoDataError);
        goto bail;
    }
    if (buffer_size <= 0) {
        PyErr_Format(PyExc_ValueError, "buffer_size (%d) invalid", buffer_size);
        goto bail;
    }
    input_read = input_remaining = input.len;
    input_pos = input.buf;

    BAIL_ON_LZ4_ERROR(LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION));

    // retrieve uncompressed data size
    BAIL_ON_LZ4_ERROR(input_size_hint = LZ4F_getFrameInfo(ctx, &frame_info, input_pos, &input_read));
    input_pos += input_read;
    input_remaining = input_read = input_remaining - input_read;
    if (frame_info.contentSize) {
        output_len = frame_info.contentSize;
        // Prevent LZ4 from buffering output - works if uncompressed size known since output does not have to be resized
        opt.stableDst = 1;
    } else {
        // uncompressed size is always at least that of compressed
        output_len = MAX((size_t) buffer_size, input_remaining);
    }

    // set up initial output buffer
    BAIL_ON_NULL(output = PyBytes_FromStringAndSize(NULL, output_len));
    BAIL_ON_NULL(output_pos = PyBytes_AsString(output));
    output_written = output_remaining = output_len;

    while (1) {
        // Decompress next chunk (Releasing GIL if input is very small could be inefficient)
        if (input_read < NOGIL_DECOMPRESS_INPUT_SIZE_THRESHOLD) {
            BAIL_ON_LZ4_ERROR(input_size_hint = LZ4F_decompress(ctx, output_pos, &output_written, input_pos,
                                                                &input_read, &opt));
        } else {
            BAIL_ON_LZ4_ERROR_NOGIL(input_size_hint = LZ4F_decompress(ctx, output_pos, &output_written, input_pos,
                                                                      &input_read, &opt));
        }
        output_pos += output_written;
        output_written = output_remaining = (output_remaining - output_written);
        // decompression complete (i.e. all data provided & fits within output buffer0
        if (!input_size_hint) {
            output_len -= output_remaining;
            break;
        }

        input_pos += input_read;
        input_read = input_remaining = (input_remaining - input_read);
        // destination too small
        if (input_remaining) {
            if (frame_info.contentSize) {
                // if frame specifies size, should never have to enlarge
                BAIL_ON_NONZERO(PyErr_WarnEx(PyExc_RuntimeWarning, "lz4frame contentSize mismatch", 2));
            }
            output_remaining += output_len;
            output_written = output_remaining;
            output_len *= 2;
            BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));
            BAIL_ON_NULL(output_pos = PyBytes_AsString(output));
            output_pos += (output_len - output_remaining);
        // insufficient data
        } else {
            PyErr_SetString(PyExc_ValueError, "frame incomplete");
            goto bail;
        }
    }
    BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));
    PyBuffer_Release(&input);
    input_held = 0;
    LZ4F_freeDecompressionContext(ctx);

    return output;

bail:
    if (input_held) {
        PyBuffer_Release(&input);
    }
    Py_XDECREF(output);
    LZ4F_freeDecompressionContext(ctx);
    return NULL;
}

/******************************************************************************/

static void _cctx_capsule_destructor(PyObject *py_ctx) {
    _lz4f_cctx_t *cctx = (_lz4f_cctx_t*)PyCapsule_GetPointer(py_ctx, COMPRESSION_CAPSULE_NAME);
    if (NULL != cctx) {
        // ignoring errors here since shouldn't throw exception in destructor
        LZ4F_freeCompressionContext(cctx->ctx);
#ifdef WITH_THREAD
        PyThread_free_lock(cctx->lock);
#endif
        PyMem_Del(cctx);
    }
}

static void _dctx_capsule_destructor(PyObject *py_ctx) {
    _lz4f_dctx_t *dctx = (_lz4f_dctx_t*)PyCapsule_GetPointer(py_ctx, DECOMPRESSION_CAPSULE_NAME);
    if (NULL != dctx) {
        // ignoring errors here since shouldn't throw exception in destructor
        LZ4F_freeDecompressionContext(dctx->ctx);
#ifdef WITH_THREAD
        PyThread_free_lock(dctx->lock);
#endif
        PyMem_Del(dctx);
    }
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_create_compression_context__doc__,
"create_compression_context() -> PyCapsule\n"
"\n"
"Create compression context for use in chunked compression.\n");
#define FUNC_DEF_CREATE_CCTX {"create_compression_context", _lz4framed_create_compression_context, METH_NOARGS,\
                              _lz4framed_create_compression_context__doc__}
static PyObject*
_lz4framed_create_compression_context(PyObject *self, PyObject *args) {
    _lz4f_cctx_t *cctx = NULL;
    PyObject *ctx_capsule = NULL;
    UNUSED(self);
    UNUSED(args);

    if (NULL == (cctx = PyMem_New(_lz4f_cctx_t, 1))) {
        PyErr_NoMemory();
        goto bail;
    }
    cctx->ctx = NULL;
    cctx->prefs = prefs_defaults;
#ifdef WITH_THREAD
    if (NULL == (cctx->lock = PyThread_allocate_lock())) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to allocate lock");
        goto bail;
    }
#endif
    BAIL_ON_LZ4_ERROR(LZ4F_createCompressionContext(&(cctx->ctx), LZ4F_VERSION));
    BAIL_ON_NULL(ctx_capsule = PyCapsule_New(cctx, COMPRESSION_CAPSULE_NAME, _cctx_capsule_destructor));
    return ctx_capsule;

bail:
    // this must NOT be freed once capsule exists (since destructor responsible for freeing)
    if (cctx) {
        LZ4F_freeCompressionContext(cctx->ctx);
#ifdef WITH_THREAD
        if (cctx->lock) {
            PyThread_free_lock(cctx->lock);
        }
#endif
        PyMem_Del(cctx);
    }
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_create_decompression_context__doc__,
"create_decompression_context() -> PyCapsule\n"
"\n"
"Create decompression context for use in chunked decompression.\n");
#define FUNC_DEF_CREATE_DCTX {"create_decompression_context", _lz4framed_create_decompression_context, METH_NOARGS,\
                              _lz4framed_create_decompression_context__doc__}
static PyObject*
_lz4framed_create_decompression_context(PyObject *self, PyObject *args) {
    _lz4f_dctx_t *dctx = NULL;
    PyObject *dctx_capsule;
    UNUSED(self);
    UNUSED(args);

    if (NULL == (dctx = PyMem_New(_lz4f_dctx_t, 1))) {
        PyErr_NoMemory();
        goto bail;
    }
    dctx->ctx = NULL;
#ifdef WITH_THREAD
    if (NULL == (dctx->lock = PyThread_allocate_lock())) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to allocate lock");
        goto bail;
    }
#endif
    BAIL_ON_LZ4_ERROR(LZ4F_createDecompressionContext(&(dctx->ctx), LZ4F_VERSION));
    BAIL_ON_NULL(dctx_capsule = PyCapsule_New(dctx, DECOMPRESSION_CAPSULE_NAME, _dctx_capsule_destructor));
    return dctx_capsule;

bail:
    // this must NOT be freed once capsule exists (since destructor responsible for freeing)
    if (dctx) {
        LZ4F_freeDecompressionContext(dctx->ctx);
#ifdef WITH_THREAD
        if (dctx->lock) {
            PyThread_free_lock(dctx->lock);
        }
#endif
        PyMem_Del(dctx);
    }
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_compress_begin__doc__,
"compress_begin(ctx, block_size_id=LZ4F_BLOCKSIZE_DEFAULT, block_mode_linked=True,\n"
"               checksum=False, autoflush=False, level=0, block_checksum=False) -> bytes\n"
"\n"
"Generates and returns frame header, sets compression options.\n"
"\n"
"Args:\n"
"    ctx: Compression context\n"
"    block_size_id (int): Compression block size identifier, one of the\n"
"                         LZ4F_BLOCKSIZE_* constants. Use get_block_size() to\n"
"                         determine size in bytes.\n"
"    block_mode_linked (bool): Whether compression blocks are linked\n"
"    checksum (bool): Whether to produce frame checksum\n"
"    autoflush (bool): Whether to flush output on update() calls rather than buffering\n"
"                      incomplete blocks internally.\n"
"    level (int): Compression level. Values lower than LZ4F_COMPRESSION_MIN_HC use fast\n"
"                 compression. Recommended range for hc compression is between 4 and 9,\n"
"                 with a maximum of LZ4F_COMPRESSION_MAX.\n"
"    block_checksum (bool): Whether to produce checksum after each block.\n"
"\n"
"Raises:\n"
"    Lz4FramedError: If a compression failure occured");
#define FUNC_DEF_COMPRESS_BEGIN {"compress_begin", (PyCFunction)_lz4framed_compress_begin,\
                                 METH_VARARGS | METH_KEYWORDS, _lz4framed_compress_begin__doc__}
static PyObject*
_lz4framed_compress_begin(PyObject *self, PyObject *args, PyObject *kwargs) {
    static const char *format = "O|iiiiii:compress_begin";
    static char *keywords[] = {"ctx", "block_size_id", "block_mode_linked", "checksum", "autoflush", "level",
                               "block_checksum", NULL};

    _lz4f_cctx_t *cctx = NULL;
    PyObject *ctx_capsule;
    int block_id = LZ4F_default;
    int block_mode_linked = 1;
    int block_checksum = 0;
    int checksum = 0;
    int autoflush = 0;
    int compression_level = LZ4_COMPRESSION_MIN;
    PyObject *output = NULL;
    char *output_str;
    size_t output_len = LZ4F_HEADER_SIZE_MAX;
    LZ4FRAMED_LOCK_FLAG;
    UNUSED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, format, keywords, &ctx_capsule, &block_id, &block_mode_linked,
                                     &checksum, &autoflush, &compression_level, &block_checksum)) {
        goto bail;
    }
    if (!PyCapsule_IsValid(ctx_capsule, COMPRESSION_CAPSULE_NAME)) {
        PyErr_SetString(PyExc_ValueError, "ctx invalid");
        goto bail;
    }
    if (!_valid_lz4f_block_size_id(block_id)) {
        PyErr_Format(PyExc_ValueError, "block_size_id (%d) invalid", block_id);
        goto bail;
    }
    if (compression_level > LZ4_COMPRESSION_MAX) {
        PyErr_Format(PyExc_ValueError, "level (%d) invalid", compression_level);
        goto bail;
    }

    // Guaranteed to succeed due to PyCapsule_IsValid check above
    cctx = PyCapsule_GetPointer(ctx_capsule, COMPRESSION_CAPSULE_NAME);

    ENTER_LZ4FRAMED(cctx);

    cctx->prefs.frameInfo.blockMode = block_mode_linked ? LZ4F_blockLinked : LZ4F_blockIndependent;
    cctx->prefs.frameInfo.blockSizeID = block_id;
    cctx->prefs.frameInfo.blockChecksumFlag = block_checksum ? LZ4F_blockChecksumEnabled : LZ4F_noBlockChecksum;
    cctx->prefs.frameInfo.contentChecksumFlag = checksum ? LZ4F_contentChecksumEnabled : LZ4F_noContentChecksum;
    cctx->prefs.compressionLevel = compression_level;
    cctx->prefs.autoFlush = autoflush ? 1 : 0;

    BAIL_ON_NULL(output = PyBytes_FromStringAndSize(NULL, output_len));
    BAIL_ON_NULL(output_str = PyBytes_AsString(output));

    // not worth releasing GIL here since only writing header
    BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressBegin(cctx->ctx, output_str, output_len, &(cctx->prefs)));

    EXIT_LZ4FRAMED(cctx);

    BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));
    return output;

bail:
    EXIT_LZ4FRAMED(cctx);
    Py_XDECREF(output);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_compress_update__doc__,
"compress_update(ctx, b) -> bytes\n"
"\n"
"Compresses and returns the given data. Note: return can be zero-length if autoflush\n"
"parameter is not set via compress_begin(). Once all data has been compressed,\n"
"compress_end() must be called (to flush any remaining data and finalise frame.\n"
"\n"
"Args:\n"
"    ctx: Compression context\n"
"    b (bytes-like object): The object containing lz4-framed data to decompress\n"
"\n"
"Raises:\n"
"    LZ4FNoDataError: If provided data is of zero length. (Useful for ending compression loop.)\n"
"    Lz4FramedError: If a compression failure occured");
#define FUNC_DEF_COMPRESS_UPDATE {"compress_update", (PyCFunction)_lz4framed_compress_update, METH_VARARGS,\
                                  _lz4framed_compress_update__doc__}
static PyObject*
_lz4framed_compress_update(PyObject *self, PyObject *args) {
#if PY_MAJOR_VERSION >= 3
    static const char *format = "Oy*:compress_update";
#else
    static const char *format = "Os*:compress_update";
#endif
    _lz4f_cctx_t *cctx = NULL;
    PyObject *ctx_capsule;
    Py_buffer input;
    int input_held = 0; // whether Py_buffer (input) needs to be released
    PyObject *output = NULL;
    char *output_str;
    size_t output_len;
    LZ4FRAMED_LOCK_FLAG;
    UNUSED(self);

    if (!PyArg_ParseTuple(args, format, &ctx_capsule, &input)) {
        goto bail;
    }
    if (!PyCapsule_IsValid(ctx_capsule, COMPRESSION_CAPSULE_NAME)) {
        PyErr_SetString(PyExc_ValueError, "ctx invalid");
        goto bail;
    }
    if (!PyBuffer_IsContiguous(&input, 'C')) {
        PyErr_SetString(PyExc_ValueError, "input not contiguous");
        goto bail;
    }
    if (input.len <= 0) {
        PyErr_SetNone(LZ4FNoDataError);
        goto bail;
    }
    // Guaranteed to succeed due to PyCapsule_IsValid check above
    cctx = PyCapsule_GetPointer(ctx_capsule, COMPRESSION_CAPSULE_NAME);

    ENTER_LZ4FRAMED(cctx);

    BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressBound(input.len, &(cctx->prefs)));
    BAIL_ON_NULL(output = PyBytes_FromStringAndSize(NULL, output_len));
    BAIL_ON_NULL(output_str = PyBytes_AsString(output));

    if (input.len < NOGIL_COMPRESS_INPUT_SIZE_THRESHOLD) {
        BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressUpdate(cctx->ctx, output_str, output_len, input.buf, input.len,
                                                           NULL));
    } else {
        BAIL_ON_LZ4_ERROR_NOGIL(output_len = LZ4F_compressUpdate(cctx->ctx, output_str, output_len, input.buf,
                                                                 input.len, NULL));
    }
    EXIT_LZ4FRAMED(cctx);
    BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));
    PyBuffer_Release(&input);
    input_held = 0;
    return output;

bail:
    EXIT_LZ4FRAMED(cctx);
    if (input_held) {
        PyBuffer_Release(&input);
    }
    Py_XDECREF(output);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_compress_end__doc__,
"compress_end(ctx) -> bytes\n"
"\n"
"Flushes any remaining compressed data, finalises frame and returns said data. After\n"
"successful compression the context can be re-used for another frame. Note: Calling\n"
"this function without having written any data (via compress_update()) will produce\n"
"an invalid frame.\n"
"\n"
"Args:\n"
"    ctx: Compression context\n"
"\n"
"Raises:\n"
"    Lz4FramedError: If a compression failure occured");
#define FUNC_DEF_COMPRESS_END {"compress_end", (PyCFunction)_lz4framed_compress_end, METH_O,\
                               _lz4framed_compress_end__doc__}
static PyObject*
_lz4framed_compress_end(PyObject *self, PyObject *arg) {
    _lz4f_cctx_t *cctx = NULL;
    PyObject *output = NULL;
    char *output_str;
    size_t output_len;
    LZ4FRAMED_LOCK_FLAG;
    UNUSED(self);

    if (!PyCapsule_IsValid(arg, COMPRESSION_CAPSULE_NAME)) {
        PyErr_SetString(PyExc_ValueError, "ctx invalid");
        goto bail;
    }
    // Guaranteed to succeed due to PyCapsule_IsValid check above
    cctx = PyCapsule_GetPointer(arg, COMPRESSION_CAPSULE_NAME);

    ENTER_LZ4FRAMED(cctx);

    BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressBound(0, &(cctx->prefs)));
    BAIL_ON_NULL(output = PyBytes_FromStringAndSize(NULL, output_len));
    BAIL_ON_NULL(output_str = PyBytes_AsString(output));

    // not worth releasing GIL since should have less than a block left to write
    BAIL_ON_LZ4_ERROR(output_len = LZ4F_compressEnd(cctx->ctx, output_str, output_len, NULL));

    EXIT_LZ4FRAMED(cctx);

    BAIL_ON_NONZERO(_PyBytes_Resize(&output, output_len));
    return output;

bail:
    EXIT_LZ4FRAMED(cctx);
    Py_XDECREF(output);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_get_frame_info__doc__,
"get_frame_info(ctx) -> dict\n"
"\n"
"Retrieves frame header information. This method can be called at any point during the\n"
"decompression process. If the header has not been parsed yet due to lack of data, one can\n"
"expect an Lz4FramedError exception with error code LZ4F_ERROR_HEADER_INCOMPLETE. On success\n"
"the method returns a dict with the following keys:\n"
"    input_hint (int)         - How many bytes to provide to next decompress() call for optimal\n"
"                               performance (due to not having to use internal buffers\n"
"    length (int)             - Uncompressed length of data (or zero if unknown)\n"
"    block_size_id (int)      - One of LZ4F_BLOCKSIZE_* constants\n"
"    block_mode_linked (bool) - Whether blocks in frame are linked\n"
"    checksum (bool)          - Whether the frame has a checksum (which will be verified)\n"
"\n"
"Args:\n"
"    ctx: Decompression context\n"
"\n"
"Raises:\n"
"    Lz4FramedError: If a compression failure occured");
#define FUNC_DEF_GET_FRAME_INFO {"get_frame_info", (PyCFunction)_lz4framed_get_frame_info, METH_O,\
                                 _lz4framed_get_frame_info__doc__}
static PyObject*
_lz4framed_get_frame_info(PyObject *self, PyObject *arg) {
    _lz4f_dctx_t *dctx = NULL;
    LZ4F_frameInfo_t frameInfo;
    size_t input_hint;
    size_t input_read = 0;
    PyObject *dict = NULL;
    PyObject *item = NULL;
    LZ4FRAMED_LOCK_FLAG;
    UNUSED(self);

    if (!PyCapsule_IsValid(arg, DECOMPRESSION_CAPSULE_NAME)) {
        PyErr_SetString(PyExc_ValueError, "ctx invalid");
        goto bail;
    }
    // Guaranteed to succeed due to PyCapsule_IsValid check above
    dctx = PyCapsule_GetPointer(arg, DECOMPRESSION_CAPSULE_NAME);

    ENTER_LZ4FRAMED(dctx);

    BAIL_ON_LZ4_ERROR(input_hint = LZ4F_getFrameInfo(dctx->ctx, &frameInfo, NULL, &input_read));

    BAIL_ON_NULL(dict = PyDict_New());
    BAIL_ON_NULL(item = PyLong_FromSize_t(input_hint));
    BAIL_ON_NONZERO(PyDict_SetItemString(dict, "input_hint", item));
    Py_CLEAR(item);
    BAIL_ON_NULL(item = PyLong_FromUnsignedLongLong(frameInfo.contentSize));
    BAIL_ON_NONZERO(PyDict_SetItemString(dict, "length", item));
    Py_CLEAR(item);
    BAIL_ON_NULL(item = PyLong_FromLong(frameInfo.blockSizeID));
    BAIL_ON_NONZERO(PyDict_SetItemString(dict, "block_size_id", item));
    Py_CLEAR(item);
    BAIL_ON_NULL(item = PyBool_FromLong(frameInfo.blockMode == LZ4F_blockLinked));
    BAIL_ON_NONZERO(PyDict_SetItemString(dict, "block_mode_linked", item));
    Py_CLEAR(item);
    BAIL_ON_NULL(item = PyBool_FromLong(frameInfo.contentChecksumFlag == LZ4F_contentChecksumEnabled));
    BAIL_ON_NONZERO(PyDict_SetItemString(dict, "checksum", item));
    Py_CLEAR(item);

    EXIT_LZ4FRAMED(dctx);

    return dict;

bail:
    EXIT_LZ4FRAMED(dctx);
    // necessary for item if dict assignment fails
    Py_XDECREF(item);
    Py_XDECREF(dict);
    return NULL;
}

/******************************************************************************/

PyDoc_STRVAR(_lz4framed_decompress_update__doc__,
"decompress_update(ctx, b, chunk_len=65536) -> list\n"
"\n"
"Decompresses parts of an lz4 frame from data given in *b*, returning the\n"
"uncompressed result as a list of chunks, with the last element being input_hint\n"
"(i.e. how many bytes to ideally expect on the next call). Once input_hint is\n"
"zero, decompression of the whole frame is complete. Note: Some calls to this\n"
"function may return no chunks if they are incomplete.\n"
"Args:\n"
"    ctx: Decompression context\n"
"    b (bytes-like object): The object containing lz4-framed data to decompress\n"
"    chunk_len (int): Size of uncompressed chunks in bytes. If not all of the\n"
"                     data fits in one chunk, multiple will be used. Ideally\n"
"                     only one chunk is required per call of this method - this can\n"
"                     be determined from block_size_id via get_frame_info() call."
"\n"
"Raises:\n"
"    Lz4FramedError: If a decompression failure occured");
#define FUNC_DEF_DECOMPRESS_UPDATE {"decompress_update", (PyCFunction)_lz4framed_decompress_update,\
                                    METH_VARARGS | METH_KEYWORDS, _lz4framed_decompress_update__doc__}
static PyObject*
_lz4framed_decompress_update(PyObject *self, PyObject *args, PyObject *kwargs) {
#if PY_MAJOR_VERSION >= 3
    static const char *format = "Oy*|i:decompress_update";
#else
    static const char *format = "Os*|i:decompress_update";
#endif
    static char *keywords[] = {"ctx", "b", "chunk_len", NULL};

    _lz4f_dctx_t *dctx = NULL;
    PyObject *dctx_capsule;
    Py_buffer input;
    int input_held = 0;              // whether Py_buffer (input) needs to be released
    const char *input_pos;           // position in input
    size_t input_remaining;          // bytes remaining in input
    size_t input_read;               // used by LZ4 functions to indicate how many bytes were / can be read
    size_t input_size_hint = 1;      // LZ4 hint to how many bytes make up the remaining block + next header
    size_t chunk_len = 65536;        // size of chunks
    PyObject *list = NULL;           // function return
    PyObject *size_hint = NULL;      // python object of input_size_hint
    PyObject *chunk = NULL ;
    char *chunk_pos = NULL ;         // position in current chunk
    size_t chunk_remaining;          // space remaining in chunk
    size_t chunk_written;            // used by lz4 to indicate how much has been written
    LZ4FRAMED_LOCK_FLAG;
    UNUSED(self);

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, format, keywords, &dctx_capsule, &input, &chunk_len)) {
        goto bail;
    }
    if (!PyCapsule_IsValid(dctx_capsule, DECOMPRESSION_CAPSULE_NAME)) {
        PyErr_SetString(PyExc_ValueError, "ctx invalid");
        goto bail;
    }
    if (!PyBuffer_IsContiguous(&input, 'C')) {
        PyErr_SetString(PyExc_ValueError, "input not contiguous");
        goto bail;
    }
    if (input.len <= 0) {
        PyErr_SetNone(LZ4FNoDataError);
        goto bail;
    }
    if (chunk_len <= 0) {
        PyErr_SetString(PyExc_ValueError, "chunk_len invalid");
        goto bail;
    }
    // Guaranteed to succeed due to PyCapsule_IsValid check above
    dctx = PyCapsule_GetPointer(dctx_capsule, DECOMPRESSION_CAPSULE_NAME);

    input_read = input_remaining = input.len;
    input_pos = input.buf;

    // output list
    BAIL_ON_NULL(list = PyList_New(0));

    // first chunk
    BAIL_ON_NULL(chunk = PyBytes_FromStringAndSize(NULL, chunk_len));
    BAIL_ON_NULL(chunk_pos = PyBytes_AsString(chunk));
    chunk_written = chunk_remaining = chunk_len;

    ENTER_LZ4FRAMED(dctx);

    while (input_remaining && input_size_hint) {
        // add another chunk for more data when current one full
        if (!chunk_remaining) {
            // append previous (full) chunk to list
            BAIL_ON_NONZERO(PyList_Append(list, chunk));
            Py_CLEAR(chunk);
            // create next chunk
            BAIL_ON_NULL(chunk = PyBytes_FromStringAndSize(NULL, chunk_len));
            BAIL_ON_NULL(chunk_pos = PyBytes_AsString(chunk));
            chunk_written = chunk_remaining = chunk_len;
        }
        if (chunk_written < NOGIL_DECOMPRESS_OUTPUT_SIZE_THRESHOLD) {
            BAIL_ON_LZ4_ERROR(input_size_hint = LZ4F_decompress(dctx->ctx, chunk_pos, &chunk_written, input_pos,
                                                                &input_read, NULL));
        } else {
            BAIL_ON_LZ4_ERROR_NOGIL(input_size_hint = LZ4F_decompress(dctx->ctx, chunk_pos, &chunk_written, input_pos,
                                                                      &input_read, NULL));
        }
        chunk_pos += chunk_written;
        chunk_written = chunk_remaining = (chunk_remaining - chunk_written);
        input_pos += input_read;
        input_read = input_remaining = (input_remaining - input_read);
    }

    EXIT_LZ4FRAMED(dctx);

    // append & reduce size of final chunk (if contains any data)
    if (chunk_remaining < chunk_len) {
        BAIL_ON_NONZERO(_PyBytes_Resize(&chunk, chunk_len - chunk_remaining));
        BAIL_ON_NONZERO(PyList_Append(list, chunk));
    }
    // append input size hint to list
    BAIL_ON_NULL(size_hint = PyLong_FromSize_t(input_size_hint));
    BAIL_ON_NONZERO(PyList_Append(list, size_hint));
    PyBuffer_Release(&input);
    input_held = 0;
    Py_CLEAR(chunk);
    Py_CLEAR(size_hint);

    return list;

bail:
    EXIT_LZ4FRAMED(dctx);
    if (input_held) {
        PyBuffer_Release(&input);
    }
    Py_XDECREF(chunk);
    Py_XDECREF(size_hint);
    Py_XDECREF(list);
    return NULL;
}


/******************************************************************************/

static PyMethodDef Lz4framedMethods[] = {
    FUNC_DEF_GET_BLOCK_SIZE, FUNC_DEF_COMPRESS, FUNC_DEF_DECOMPRESS, FUNC_DEF_CREATE_CCTX, FUNC_DEF_CREATE_DCTX,
    FUNC_DEF_COMPRESS_BEGIN, FUNC_DEF_COMPRESS_UPDATE, FUNC_DEF_COMPRESS_END, FUNC_DEF_GET_FRAME_INFO,
    FUNC_DEF_DECOMPRESS_UPDATE,
    {NULL, NULL, 0, NULL}
};

struct module_state {
    PyObject *error;
};

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

#if PY_MAJOR_VERSION >= 3

static int myextension_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int myextension_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}


static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_lz4framed",
        NULL,
        sizeof(struct module_state),
        Lz4framedMethods,
        NULL,
        myextension_traverse,
        myextension_clear,
        NULL
};

#define INITERROR return NULL
PyObject*
PyInit__lz4framed(void)

#else
#define INITERROR return

void
init_lz4framed(void)
#endif
{
    struct module_state *state = NULL;
#if PY_MAJOR_VERSION >= 3
    PyObject *module = PyModule_Create(&moduledef);
#else
    PyObject *module = Py_InitModule("_lz4framed", Lz4framedMethods);
#endif

    BAIL_ON_NULL(module);
    BAIL_ON_NULL(state = GETSTATE(module));

    BAIL_ON_NULL(state->error = PyErr_NewException("_lz4framed.Error", NULL, NULL));
    BAIL_ON_NULL(LZ4FError = PyErr_NewExceptionWithDoc("_lz4framed.Lz4FramedError", __lz4f_error__doc__, NULL, NULL));
    BAIL_ON_NULL(LZ4FNoDataError = PyErr_NewExceptionWithDoc("_lz4framed.Lz4FramedNoDataError",
                                                             __lz4f_no_data_error__doc__, NULL, NULL));
    Py_INCREF(LZ4FError);
    Py_INCREF(LZ4FNoDataError);

    // non-zero returns indicate error
    if (PyModule_AddObject(module, "Lz4FramedError", LZ4FError) ||
        PyModule_AddObject(module, "Lz4FramedNoDataError", LZ4FNoDataError) ||
        PyModule_AddStringConstant(module, "__version__", EXPAND_AND_QUOTE(VERSION)) ||
        PyModule_AddStringConstant(module, "LZ4_VERSION", LZ4_VERSION_STRING) ||
        PyModule_AddIntMacro(module, LZ4F_VERSION) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_GENERIC) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_maxBlockSize_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_blockMode_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_contentChecksumFlag_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_compressionLevel_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_headerVersion_wrong) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_blockChecksum_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_reservedFlag_set) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_allocation_failed) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_srcSize_tooLarge) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_dstMaxSize_tooSmall) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_frameHeader_incomplete) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_frameType_unknown) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_frameSize_wrong) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_srcPtr_wrong) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_decompressionFailed) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_headerChecksum_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_contentChecksum_invalid) ||
        PyModule_AddIntMacro(module, LZ4F_ERROR_frameDecoding_alreadyStarted) ||
        PyModule_AddIntConstant(module, "LZ4F_BLOCKSIZE_DEFAULT", LZ4F_default) ||
        PyModule_AddIntConstant(module, "LZ4F_BLOCKSIZE_MAX64KB", LZ4F_max64KB) ||
        PyModule_AddIntConstant(module, "LZ4F_BLOCKSIZE_MAX256KB", LZ4F_max256KB) ||
        PyModule_AddIntConstant(module, "LZ4F_BLOCKSIZE_MAX1MB", LZ4F_max1MB) ||
        PyModule_AddIntConstant(module, "LZ4F_BLOCKSIZE_MAX4MB", LZ4F_max4MB) ||
        PyModule_AddIntConstant(module, "LZ4F_COMPRESSION_MIN", LZ4_COMPRESSION_MIN) ||
        PyModule_AddIntConstant(module, "LZ4F_COMPRESSION_MIN_HC", LZ4_COMPRESSION_MIN_HC) ||
        PyModule_AddIntConstant(module, "LZ4F_COMPRESSION_MAX", LZ4_COMPRESSION_MAX)) {
        goto bail;
    }

#if PY_MAJOR_VERSION >= 3
    return module;
#else
    return;
#endif

bail:
    Py_XINCREF(LZ4FError);
    Py_XINCREF(LZ4FNoDataError);
    Py_XDECREF(module);
    INITERROR;
}
