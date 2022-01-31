import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from setup_month import MonthSheet


class TestMonthSheet:

    def test_init(self):
        ms = MonthSheet(full_sheet=None, path=None, mode='testing')
        assert ms.test_message == 'hi'