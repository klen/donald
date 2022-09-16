import pytest
from redis import Redis


@pytest.fixture
def check(tmp_path):
    def checker():
        path = tmp_path / "result.txt"
        if not path.exists():
            return None
        with open(path) as f:
            return f.read().splitlines()

    def clear():
        path = tmp_path / "result.txt"
        if path.exists():
            path.unlink()

    checker.path = tmp_path
    checker.clear = clear
    return checker


@pytest.fixture
def redis(redis_url):
    return Redis.from_url(redis_url)
