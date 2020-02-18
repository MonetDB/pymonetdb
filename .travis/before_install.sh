#!/bin/bash -ve

export DBFARM=/var/lib/monetdb

# start database
sudo mkdir -p -m 770 ${DBFARM}
sudo chown -R monetdb.monetdb ${DBFARM}
sudo -u monetdb monetdbd create ${DBFARM}
sudo -u monetdb monetdbd set control=yes ${DBFARM}
sudo -u monetdb monetdbd set passphrase=testdb ${DBFARM}
sudo -u monetdb monetdbd start ${DBFARM}

# set up test database
sudo -u monetdb monetdb create demo
sudo -u monetdb monetdb set embedpy3=true demo
sudo -u monetdb monetdb release demo
sudo -u monetdb monetdb start demo
sudo cat /var/lib/monetdb/merovingian.log

# install python test requirements
pip install -r tests/requirements.txt
