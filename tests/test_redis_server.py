import redis

from redis import exceptions
from pytest_dependency_fixtures import RedisServer, redis_server


def test_redis_server_conn_and_get_set(redis_server: RedisServer):
    redis_client = redis.Redis(
        host="127.0.0.1",
        port=6379,
        retry_on_timeout=True,
        retry_on_error=[
            ConnectionRefusedError,
            ConnectionError,
            exceptions.ConnectionError,
        ],
    )

    test_value = "hello there"

    redis_client.set("redis-testing", test_value)

    response: bytes = redis_client.get("redis-testing")

    assert test_value == response.decode()
