# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
# Copyright (c) 2010-2012 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2010-2011, Eucalyptus Systems, Inc.
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

# this is here for backward compatibility
# originally, the SNSConnection class was defined here
from boto.sns.connection import SNSConnection
from boto.regioninfo import RegionInfo, get_regions
from boto.regioninfo import connect


def regions():
    """
    Get all available regions for the SNS service.

    :rtype: list
    :return: A list of :class:`boto.regioninfo.RegionInfo` instances
    """
    return get_regions('sns', connection_cls=SNSConnection)


def connect_to_region(region_name, **kw_params):
    """
    Given a valid region name, return a
    :class:`boto.sns.connection.SNSConnection`.

    :type: str
    :param region_name: The name of the region to connect to.

    :rtype: :class:`boto.sns.connection.SNSConnection` or ``None``
    :return: A connection to the given region, or None if an invalid region
             name is given
    """
    return connect('sns', region_name,
                   connection_cls=SNSConnection, **kw_params)
