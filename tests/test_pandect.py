#!/usr/bin/env python

"""Tests for `pandect` package."""


import unittest

import pandect


class TestPandect(unittest.TestCase):
    """Tests for `pandect` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_load_dta(self):
        """Test loading dta data file"""
        data, meta = pandect.load('tests/data/data.dta')
        assert len(data) == 100
        assert len(list(data)) == 4
        assert meta.column_names == ['a', 'b', 'c', 'd']

    def test_load_sav(self):
        """Test loading sav data file"""
        data, meta = pandect.load('tests/data/data.sav')
        assert len(data) == 100
        assert len(list(data)) == 4
        assert meta.column_names == ['a', 'b', 'c', 'd']

    def test_load_csv(self):
        """Test loading csv data file"""
        data, meta = pandect.load('tests/data/data.csv')
        assert len(data) == 100
        assert len(list(data)) == 4

    def test_load_xlsx(self):
        """Test loading xlsx data file"""
        data, meta = pandect.load('tests/data/data.xlsx')
        assert len(data) == 100
        assert len(list(data)) == 4
