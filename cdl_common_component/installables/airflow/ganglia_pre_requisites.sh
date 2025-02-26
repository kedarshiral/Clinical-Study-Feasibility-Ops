# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-1.9.8-linux-x86_64.tar.bz2
bunzip2 phantomjs-1.9.8-linux-x86_64.tar.bz2
tar -xvf phantomjs-1.9.8-linux-x86_64.tar
mv phantomjs-1.9.8-linux-x86_64 phantomjs_dir

sudo pip install boto3
sudo pip install pytz
sudo pip install selenium
sudo pip install fpdf
sudo pip install pyPdf

