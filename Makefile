# This file is intended for development purposes and should not be used to install pymonetdb

.PHONY: doc clean test

all: test

venv/:
	python3 -m venv venv
	venv/bin/pip install --upgrade pip wheel

venv/installed: venv/
	venv/bin/pip install -e ".[test,doc]"
	touch venv/installed

setup: venv/installed

test: setup
	venv/bin/pytest

docker-wheels:
	manylinux2010/outside.sh

clean: venv/
	venv/bin/python3 setup.py clean
	rm -rf build dist *.egg-info .eggs  .*_cache venv/ doc/_build

venv/bin/mypy: setup
	venv/bin/pip install mypy
	touch venv/bin/mypy

venv/bin/pycodestyle: setup
	venv/bin/pip install pycodestyle
	touch venv/bin/pycodestyle

pycodestyle: venv/bin/pycodestyle
	venv/bin/pycodestyle pymonetdb tests

mypy: venv/bin/mypy
	venv/bin/mypy pymonetdb tests

venv/bin/delocate-wheel: setup
	venv/bin/pip install delocate

delocate: venv/bin/delocate-wheel
	venv/bin/delocate-wheel -v dist/*.whl

venv/bin/twine: setup
	venv/bin/pip install twine

sdist: setup
	venv/bin/python setup.py build sdist

wheel: setup
	venv/bin/python setup.py build bdist_wheel

twine: venv/bin/twine
	venv/bin/twine upload dist/*.whl dist/*.tar.gz

doc: setup
	PATH=$${PATH}:${CURDIR}/venv/bin $(MAKE) -C doc html

venv/bin/flake8: setup
	venv/bin/pip install flake8
	touch venv/bin/flake8

flake8: venv/bin/flake8
	venv/bin/flake8 --count --select=E9,F63,F7,F82 --show-source --statistics pymonetdb tests
	venv/bin/flake8 --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics pymonetdb tests

checks: mypy pycodestyle flake8