"""
Test the URL parser and related utilities
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import os
from typing import Optional, Tuple
from unittest import TestCase, skipIf

from pymonetdb.target import ALL_FIELDS, Target, strict_percent_decode

HERE = os.path.dirname(__file__)

FIELDS = set(ALL_FIELDS)

DUMMY_TEST_CASE = """\
PARSE monetdb://?binary=01
EXPECT binary=1
"""


class TestMonetDBURL(TestCase):
    @skipIf(DUMMY_TEST_CASE.strip() == "", reason="only while debugging")
    def test_dummy(self):
        self.run_test("<dummy>", 1, DUMMY_TEST_CASE.splitlines())

    def test_standardized_tests(self):
        self.run_tests("tests.md")

    def run_tests(self, filename, lines=None):
        if lines is None:
            with open(os.path.join(HERE, filename)) as f:
                lines = f.readlines()

        lineno = 0
        header = None
        started = None
        testcase = []
        for line in lines:
            lineno += 1
            line = line.rstrip()
            if started is None:
                if line == "```test":
                    started = lineno
                elif line.startswith("#"):
                    header = line.lstrip("# ")
                continue
            if line != "```":
                testcase.append(line)
                continue

            props = dict(file=filename, lineno=started, header=header)
            props = dict((k, v) for k, v in props.items() if v is not None)
            with self.subTest(**props):
                self.run_test(filename, started + 1, testcase)

            started = None
            del testcase[:]

    def run_test(self, path, lineno, lines):
        target = Target()
        for i, line in enumerate(lines):
            try:
                self.run_line(target, line)
            except Exception as e:
                if hasattr(e, "add_note"):
                    # since Python 3.11
                    e.add_note(f"(on line {lineno + i} of {path})")
                raise e

    def run_line(self, target: Target, line):
        lo = line.upper()
        if lo.startswith("SET "):
            self.run_set(target, line[4:])
        elif lo.startswith("PARSE "):
            self.run_parse(target, line[6:], True)
        elif lo.startswith("REJECT "):
            self.run_parse(target, line[7:], False)
        elif lo.startswith("EXPECT "):
            self.run_expect(target, line[7:])
        else:
            self.fail("cannot parse: " + line)

    def run_parse(self, target: Target, url: str, should_succeed: bool):
        try:
            target.parse_url(url)
            if not should_succeed:
                self.fail("expected an error")
        except ValueError as e:
            if should_succeed:
                raise e

    def run_set(self, target: Target, prop: str):
        name, value = self.parse_property(prop)
        target.set_from_text(name, value, only_params=False)

    def run_expect(self, target: Target, property: str):
        name, expected = self.parse_property(property)
        actual = target.get_as_text(name)
        self.assertEqual(expected, actual)

    def parse_property(self, prop: str) -> Tuple[str, Optional[str]]:
        eq = prop.find("=")
        if prop.upper().startswith("NO "):
            if eq >= 0:
                self.fail("cannot have = after NO")
            key = prop[3:]
            value = None
        else:
            if eq < 0:
                self.fail("expected =")
            key = prop[:eq]
            eq += 1
            value = prop[eq:]
        return (key, value)


class TestPercentDecode(TestCase):
    def test_noescapes(self):
        value = strict_percent_decode("banana")
        self.assertEqual(value, "banana")

    def test_good_escape(self):
        value = strict_percent_decode("ban%41na")
        self.assertEqual(value, "banAna")

    def test_lowercase_escape(self):
        value = strict_percent_decode("ban%2fna")
        self.assertEqual(value, "ban/na")

    def test_uppercase_escape(self):
        value = strict_percent_decode("ban%2Fna")
        self.assertEqual(value, "ban/na")

    def test_bad_escape(self):
        with self.assertRaises(ValueError):
            strict_percent_decode("ban%x1na")

    def test_incomplete_escape1(self):
        with self.assertRaises(ValueError):
            strict_percent_decode("banana%2")

    def test_incomplete_escape0(self):
        with self.assertRaises(ValueError):
            strict_percent_decode("banana%")

    def test_double_percent(self):
        # %% is not the right way to encode %
        with self.assertRaises(ValueError):
            strict_percent_decode("ban%%ana")
