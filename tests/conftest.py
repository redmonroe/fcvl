from pytest import fixture


def pytest_addoption(parser):
    parser.addoption(
        "--write",
        action="store"
    )

@fixture()
def write(request):
    return request.config.getoption("--write")