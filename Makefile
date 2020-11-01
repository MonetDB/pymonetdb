# This file is intended for development purposes and should not be used to install pymonetdb

all: test

venv/:
	python3 -m venv venv
	venv/bin/pip install --upgrade pip wheel

venv/installed: venv/
	venv/bin/pip install -e ".[test]"
	touch venv/installed

setup: venv/installed

test: setup
	venv/bin/pytest

docker-wheels:
	manylinux2010/outside.sh

clean: venv/
	venv/bin/python3 setup.py clean
	rm -rf build dist *.egg-info .eggs  .*_cache venv/

venv/bin/mypy: venv/
	venv/bin/pip install mypy

venv/bin/pycodestyle: venv/
	venv/bin/pip install pycodestyle

mypy: venv/bin/mypy
	venv/bin/mypy pymonetdb tests

venv/bin/delocate-wheel: venv/
	venv/bin/pip install delocate

delocate: venv/bin/delocate-wheel
	venv/bin/delocate-wheel -v dist/*.whl

venv/bin/twine: venv/
	venv/bin/pip install twine

sdist: venv/
	venv/bin/python setup.py build sdist

twine: venv/bin/twine
	venv/bin/twine upload dist/*.whl dist/*.tar.gz