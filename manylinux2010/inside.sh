#!/bin/bash
set -e -x

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install -r /io/tests/requirements.txt
    "${PYBIN}/pip" wheel /io/ -w /io/manylinux2010/
done

# Bundle external shared libraries into the wheels
for whl in /io/manylinux2010//*.whl; do
    auditwheel repair "$whl" --plat manylinux2010_x86_64 -w /io/manylinux2010/
done

# Install packages and test
for PYBIN in /opt/python/*/bin/; do
    "${PYBIN}/pip" install pymonetdb --no-index -f /io/manylinux2010/

    # for now we cant test since no DB running sinde container
    #(cd "$HOME"; "${PYBIN}/nosetests" pymonetdb)
done