# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
# Copyright (c) 2008 Chris Moyer http://coredumped.org
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
from boto.pyami.installers.ubuntu.installer import Installer

class Apache(Installer):
    """
    Install apache2, mod_python, and libapache2-svn
    """

    def install(self):
        self.run("apt-get update")
        self.run('apt-get -y install apache2', notify=True, exit_on_error=True)
        self.run('apt-get -y install libapache2-mod-python', notify=True, exit_on_error=True)
        self.run('a2enmod rewrite', notify=True, exit_on_error=True)
        self.run('a2enmod ssl', notify=True, exit_on_error=True)
        self.run('a2enmod proxy', notify=True, exit_on_error=True)
        self.run('a2enmod proxy_ajp', notify=True, exit_on_error=True)

        # Hard reboot the apache2 server to enable these module
        self.stop("apache2")
        self.start("apache2")

    def main(self):
        self.install()
