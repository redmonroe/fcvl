import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from config import Config
from setup_month import MonthSheet
from google_api_calls_abstract import GoogleApiCalls
from _pytest.monkeypatch import MonkeyPatch

monkeypatch = MonkeyPatch()

class TestMonthSheet():

    def test_init(self):
        ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing')
        assert ms.test_message == 'hi'

    def test_push_to_intake(self, monkeypatch):
        ms = MonthSheet(full_sheet=Config.TEST_RS, path=Config.TEST_RS_PATH, mode='testing')
        choice1 = 2
        choice2 = 1
        answers = iter([choice1, choice2])
         # using lambda statement for mocking
        monkeypatch.setattr('builtins.input', lambda name: next(answers))
        ms.push_to_intake()

        # get one cell

        # clear sheet
        

if __name__ == '__main__':
    test_ms = TestMonthSheet()
    test_ms.test_push_to_intake(monkeypatch)