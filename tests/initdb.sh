#!/usr/bin/env bash

set -e
set -v

DATABASE="demo"

# if not set monetdb crashes on some platforms
export LANG="en_US.UTF-8"
export LC_CTYPE=$LANG

monetdb stop $DATABASE || true
monetdb destroy -f $DATABASE || true
monetdb create $DATABASE
monetdb release $DATABASE
monetdb start $DATABASE


# monetdbd set control=yes /var/lib/monetdb/
# monetdbd set passphrase=testdb /var/lib/monetdb