#!/bin/bash -ve

# install monetdb
sudo apt-get install software-properties-common
sudo apt-get update -q
sudo sh -c "echo 'deb http://dev.monetdb.org/downloads/deb/ xenial monetdb' > /etc/apt/sources.list.d/monetdb.list"
wget --output-document=- http://dev.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -
sudo apt-get update -q
sudo apt-get install -qy monetdb5-sql monetdb-client

# start database
sudo -u monetdb mkdir -m 700 /var/lib/monetddb
sudo -u monetdb /usr/bin/monetdbd create /var/lib/monetdb
sudo -u monetdb /usr/bin/monetdbd start /var/lib/monetdb

# set up test database
sudo monetdb create demo
sudo monetdb release demo
sudo monetdbd set control=yes /var/lib/monetdb
sudo monetdbd set passphrase=testdb /var/lib/monetdb

# install python test requirements
pip install -r test/requirements.txt
