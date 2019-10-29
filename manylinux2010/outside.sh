#!/bin/bash -ve

HERE=`dirname "$0"`
cd $HERE/..

docker run --rm -v `pwd`:/io quay.io/pypa/manylinux2010_x86_64 sh /io/manylinux2010/inside.sh