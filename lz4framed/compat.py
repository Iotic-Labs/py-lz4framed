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

# pylint: disable=unused-import,invalid-name,wrong-import-order

from sys import stderr, stdout, stdin, version_info

try:
    # pylint: disable=no-name-in-module
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable  # noqa

PY2 = (version_info[0] == 2)

if PY2:
    STDIN_RAW = stdin
    STDOUT_RAW = stdout
    STDERR_RAW = stderr

else:
    STDIN_RAW = getattr(stdin, 'buffer', stdin)
    STDOUT_RAW = getattr(stdout, 'buffer', stdout)
    STDERR_RAW = getattr(stderr, 'buffer', stderr)
