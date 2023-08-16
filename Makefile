# This file is intended for development purposes and should not be used to install pymonetdb

DBFARM=dbfarm
DATABASE=demo

.PHONY: doc clean test wheel sdist dbfarm-start database-init all build

all: test doc checks build

build: wheel sdist

venv/:
	python3 -m venv venv
	venv/bin/pip install --upgrade pip wheel

venv/installed: venv/
	venv/bin/pip install setuptools
	venv/bin/pip install -e ".[test,doc]"
	touch venv/installed

setup: venv/installed

test: setup
	venv/bin/pytest

testwheel: wheel
	rm -rf testvenv
	python3 -m venv testvenv
	./testvenv/bin/pip install dist/*.whl
	./testvenv/bin/pip install pytest
	cd tests && ../testvenv/bin/pytest

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

venv/bin/twine: setup
	venv/bin/pip install twine

sdist: setup
	venv/bin/python setup.py build sdist

wheel: setup
	venv/bin/python setup.py build bdist_wheel

upload: venv/bin/twine wheel sdist
	venv/bin/twine upload dist/*.whl dist/*.tar.gz

doc: setup
	 PATH=$${PATH}:${CURDIR}/venv/bin $(MAKE) -C doc html SPHINXOPTS="-W"

venv/bin/flake8: setup
	venv/bin/pip install flake8
	touch venv/bin/flake8

flake8: venv/bin/flake8
	venv/bin/flake8 --count --select=E9,F63,F7,F82 --show-source --statistics pymonetdb tests
	venv/bin/flake8 --count --max-complexity=10 --max-line-length=127 --statistics pymonetdb tests

venv/bin/pylama: setup
	venv/bin/pip install "pylama[all]"
	touch venv/bin/pylama

pylama: venv/bin/pylama
	venv/bin/pylama pymonetdb tests

checks: mypy pycodestyle flake8

$(DBFARM):
	monetdbd create $(DBFARM)
	monetdbd start $(DBFARM)
	monetdbd set control=yes $(DBFARM)
	monetdbd set passphrase=testdb $(DBFARM)
	monetdbd stop $(DBFARM)
	monetdbd start $(DBFARM)

dbfarm-start:
	monetdbd start $(DBFARM)

database-init:
	monetdb stop $(DATABASE) || true
	monetdb destroy -f $(DATABASE) || true
	monetdb create $(DATABASE)
	monetdb release $(DATABASE)
	monetdb start $(DATABASE)
