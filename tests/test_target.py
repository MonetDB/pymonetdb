#!/usr/bin/env python3

import os
import re
from typing import List, Optional, Tuple
from unittest import TestCase
import unittest

from pymonetdb.target import VIRTUAL, Target, parse_bool


class Line(str):
    """A Line is a string that remembers which file and line number it came from"""
    file: str
    idx: int
    nr: int

    def __new__(cls, text: str, file: str, idx: int):
        line = super().__new__(cls, text)
        line.file = file
        line.idx = idx
        line.nr = idx + 1
        return line

    @property
    def location(self):
        return self.file + ":" + str(self.nr)


def read_lines(f, filename: str, start_line=0) -> List[Line]:
    """Read from 'f' and turn the lines into Lines"""
    n = start_line
    lines = []
    for s in f:
        s = s.rstrip()
        line = Line(s, filename, n)
        lines.append(line)
        n += 1
    return lines


def split_tests(lines: List[Line]) -> List[Tuple[str, List[Line]]]:
    tests: List[Tuple[str, List[Line]]] = []
    cur: Optional[List[Line]] = None
    header = None
    count = 0
    location = None
    for line in lines:
        if cur is None:
            if line.startswith("```test"):
                location = line.location
                cur = []
            elif line.startswith('#'):
                header = line.lstrip('#').strip()
                header = re.sub(r'\W+', '_', header).lower()
                count = 0
        else:
            if line.startswith("```"):
                count += 1
                name = f"{header}_{count}"
                assert len(cur) > 0
                tests.append((name, cur))
                cur = None
            else:
                cur.append(line)
    if cur is not None:
        raise Exception(f"Unclosed block at {location}")
    return tests


# Note: we programmatically add test methods to this file
# based on the contents of tests.md.
class TargetTests(TestCase):

    def test_dummy_tests(self):
        """Convenience method. Run the tests from t.md if that exists."""
        filename = 't.md'
        if not os.path.exists(filename):
            raise unittest.SkipTest(f"{filename} does not exist")
        lines = read_lines(open(filename), filename)
        tests = split_tests(lines)
        for name, test in tests:
            self.run_test(test)

    def run_test(self, test):
        target = Target()
        for line in test:
            try:
                self.apply_line(target, line)
                continue
            except AssertionError as e:
                if hasattr(e, 'add_note'):
                    e.add_note(f"At {line.location}")
                    raise
                else:
                    raise AssertionError(f"At {line.location}: {e}")
            except unittest.SkipTest:
                break
            except Exception as e:
                if hasattr(e, 'add_note'):
                    e.add_note(f"At {line.location}")
                    raise

    def apply_line(self, target: Target, line: Line):  # noqa C901
        if not line:
            return

        command, rest = line.split(None, 1)
        command = command.upper()
        if command == "PARSE":
            self.apply_parse(target, rest)
        elif command == "ACCEPT":
            self.apply_accept(target, rest)
        elif command == "REJECT":
            self.apply_reject(target, rest)
        elif command == "EXPECT":
            key, value = rest.split('=', 1)
            self.apply_expect(target, key, value)
        elif command == "SET":
            key, value = rest.split('=', 1)
            self.apply_set(target, key, value)
        elif command == "ONLY":
            impl = rest
            if impl != 'pymonetdb':
                raise unittest.SkipTest(f"only for {impl}")
        elif command == "NOT":
            impl = rest
            if impl == 'pymonetdb':
                raise unittest.SkipTest(f"not for {impl}")
        else:
            self.fail(f"Unknown command: {command}")

    def apply_parse(self, target: Target, url):
        target.parse(url)

    def apply_accept(self, target: Target, url):
        target.parse(url)
        target.validate()

    def apply_reject(self, target: Target, url):
        try:
            target.parse(url)
        except ValueError:
            return
        # last hope
        try:
            target.validate()
        except ValueError:
            return
        raise ValueError("Expected URL to be rejected")

    def apply_set(self, target: Target, key, value):
        target.set(key, value)

    def apply_expect(self, target: Target, key, expected_value):
        if key == 'valid':
            return self.apply_expect_valid(target, key, expected_value)

        if key in VIRTUAL:
            target.validate()

        if key == 'connect_binary':
            actual_value = target.connect_binary(65535)
        else:
            actual_value = target.get(key)

        self.verify_expected_value(key, expected_value, actual_value)

    def verify_expected_value(self, key, expected_value, actual_value):
        if isinstance(actual_value, bool):
            expected_value = parse_bool(expected_value)
        elif isinstance(actual_value, int):
            try:
                expected_value = int(expected_value)
            except ValueError:
                # will show up in the comparison below
                pass
        if actual_value != expected_value:
            self.fail(f"Expected {key}={expected_value!r}, found {actual_value!r}")

    def apply_expect_valid(self, target, key, expected_value):
        should_succeed = parse_bool(expected_value)
        try:
            target.validate()
            if not should_succeed:
                self.fail("Expected valid=false")
        except ValueError as e:
            if should_succeed:
                self.fail(f"Expected valid=true, got error {e}")
        return


# Magic alert!
# Read tests.md and generate test cases programmatically!
filename = os.path.join(os.path.dirname(__file__), 'tests.md')
lines = read_lines(open(filename), filename)
tests = split_tests(lines)
for name, test in tests:
    if test:
        line_nr = f"line_{test[0].nr}_"
    else:
        line_nr = ""

    def newlexicalscope(test):
        return lambda this: this.run_test(test)
    setattr(TargetTests, f"tests_md_{line_nr}{name}", newlexicalscope(test))
