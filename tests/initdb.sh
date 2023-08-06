#!/usr/bin/env bash

# Usage: initdb <DBNAME> [<FARMDIR>]

set -e -x

# DATABASE defaults to demo, DBFARM has no default
DATABASE="${1-demo}"
DBFARM="$2"


# if not set monetdb crashes on some platforms
export LANG="en_US.UTF-8"
export LC_CTYPE=$LANG

# First configure the farm
if [ -n "$DBFARM" ]; then
	test -d "$DBFARM"
	monetdbd stop "$DBFARM" || true "No problem!"
	monetdbd set control=yes "$DBFARM"
	monetdbd set passphrase=testdb "$DBFARM"
	monetdbd start "$DBFARM"
fi

# Then the database
monetdb stop "$DATABASE" || true "No problem!"
monetdb destroy -f "$DATABASE" || true "No problem!"
monetdb create "$DATABASE"
monetdb release "$DATABASE"
monetdb start "$DATABASE"
