#!/bin/bash -ve

# install monetdb
sudo apt-get install software-properties-common
sudo apt-get update -q
sudo sh -c "echo 'deb http://dev.monetdb.org/downloads/deb/ xenial monetdb' > /etc/apt/sources.list.d/monetdb.list"
wget --output-document=- http://dev.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -
sudo apt-get update -q
sudo apt-get install -qy monetdb5-sql monetdb-client

# start database
sudo mkdir -p -m 770 /var/lib/monetdb
sudo chown -R monetdb.monetdb /var/lib/monetdb
sudo -u monetdb monetdbd create /var/lib/monetdb
sudo -u monetdb monetdbd start /var/lib/monetdb
sudo -u monetdb monetdbd set control=yes /var/lib/monetdb
sudo -u monetdb monetdbd set passphrase=testdb /var/lib/monetdb

# set up test database
sudo -u monetdb monetdb create demo
sudo -u monetdb monetdb set embedpy=true demo
sudo -u monetdb monetdb release demo

# install python test requirements
pip install -r tests/requirements.txt
