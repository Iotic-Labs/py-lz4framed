# Copyright (c) 2015 Iotic Labs Ltd. All rights reserved.

# Original six.py copyright notice, on which snippets herein are based:
#
# Copyright (c) 2010-2015 Benjamin Peterson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Python v2.7 (NOT 2.6) compatibility"""

# pylint: disable=unused-import

from sys import stderr, stdout, stdin, version_info

try:
    # pylint: disable=no-name-in-module
    from collections.abc import Iterable, Mapping, Sequence
except ImportError:
    from collections import Iterable, Mapping, Sequence  # noqa

PY2 = (version_info[0] == 2)

if PY2:
    from io import StringIO as BytesIO  # noqa

    # pylint: disable=undefined-variable
    integer_types = (int, long)  # noqa
    unicode_type = unicode  # noqa
    text_types = (str, unicode)  # noqa
    bytes_types = (str,)

    def u(item):
        return unicode(item)  # noqa

    stdin_raw = stdin
    stdout_raw = stdout
    stderr_raw = stderr

else:
    from io import BytesIO  # noqa

    integer_types = (int,)
    unicode_type = str
    text_types = (str,)
    bytes_types = (bytes, bytearray)

    def u(item):
        return str(item)

    stdin_raw = stdin.buffer  # pylint: disable=no-member
    stdout_raw = stdout.buffer  # pylint: disable=no-member
    stderr_raw = stderr.buffer  # pylint: disable=no-member

if version_info[:2] == (3, 2):
    # pylint: disable=exec-used
    exec("""def raise_from(value, from_value):
    if from_value is None:
        raise value
    raise value from from_value
""")
elif version_info[:2] > (3, 2):
    # pylint: disable=exec-used
    exec("""def raise_from(value, from_value):
    raise value from from_value
""")
else:
    def raise_from(value, _):
        raise value
