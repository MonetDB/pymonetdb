#!/usr/bin/env bash

# Usage: exitdb <FARMDIR>

set -e -x

DBFARM="$1"

# if not set monetdb crashes on some platforms
export LANG="en_US.UTF-8"
export LC_CTYPE=$LANG

test -d "$DBFARM" 
	monetdbd stop "$DBFARM" || true "No problem!"
test -d "$DBFARM" 
	rm -rf "$DBFARM"
