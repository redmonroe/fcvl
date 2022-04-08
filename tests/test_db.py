import pytest

@pytest.mark.testing_db
class TestDB:

    test_message = 'hi'

    def test_init(self):
        assert self.test_message == 'hi'
        