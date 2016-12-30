#!/bin/bash -ve

# run tests
nosetests --with-coverage  --cover-package=pymonetdb
pep8 pymonetdb --ignore=E501

# build doc
if [ "${TRAVIS_PYTHON_VERSION}" = "3.5" ]; then
	cd doc
	sphinx-build -b html -d _build/doctrees -W  . _build/html
	cd ..
else
    echo "only building doc for python 3.5, this is ${TRAVIS_PYTHON_VERSION} so skipping"
fi

