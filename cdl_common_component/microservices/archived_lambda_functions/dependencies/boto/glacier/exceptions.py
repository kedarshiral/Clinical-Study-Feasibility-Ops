# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
# -*- coding: utf-8 -*-
# Copyright (c) 2012 Thomas Parslow http://almostobsolete.net/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#
from boto.compat import json


class UnexpectedHTTPResponseError(Exception):
    def __init__(self, expected_responses, response):
        self.status = response.status
        self.body = response.read()
        self.code = None
        try:
            body = json.loads(self.body)
            self.code = body["code"]
            msg = 'Expected %s, got ' % expected_responses
            msg += '(%d, code=%s, message=%s)' % (response.status,
                                                  self.code,
                                                  body["message"])
        except Exception:
            msg = 'Expected %s, got (%d, %s)' % (expected_responses,
                                                 response.status,
                                                 self.body)
        super(UnexpectedHTTPResponseError, self).__init__(msg)


class ArchiveError(Exception):
    pass


class UploadArchiveError(ArchiveError):
    pass


class DownloadArchiveError(ArchiveError):
    pass


class TreeHashDoesNotMatchError(ArchiveError):
    pass
