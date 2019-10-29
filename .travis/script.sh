#!/bin/bash -ve

# run tests
nosetests --with-coverage  --cover-package=pymonetdb

CHECK_PYTHON_VERSION="3.7"

# build doc, check code style and typing. But only once.
if [[ "${TRAVIS_PYTHON_VERSION}" = ${CHECK_PYTHON_VERSION} ]]; then
	cd doc
	sphinx-build -b html -d _build/doctrees -W  . _build/html
	cd ..

    pycodestyle pymonetdb --ignore=E501

    # mypy is only available for python >3
    pip install mypy
    mypy pymonetdb tests

else
    echo "only building doc for python ${CHECK_PYTHON_VERSION}, this is ${TRAVIS_PYTHON_VERSION} so skipping"
fi

