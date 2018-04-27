#!/usr/bin/env python
# The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
import unittest
import trytond.tests.test_tryton
from trytond.tests.test_tryton import ModuleTestCase


class StockCalculationTestCase(ModuleTestCase):
    'Test Stock Calculation module'
    module = 'stock_calculation'


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(
        StockCalculationTestCase))
    return suite
