import os
import time
import logging
import pytest
import platform
import subprocess
import typing as T

from .utils import delay


class RedisServer:

    def __init__(self) -> None:
        self.proc: T.Optional[subprocess.Popen] = None

    def start(self):
        uname = platform.uname()
        bin_name = f"redis_{uname.system}_{uname.machine}"

        full_bin_path = os.getcwd() + "/3rdparty/redis/" + bin_name

        logging.info(f"starting broker: {full_bin_path}")
        self.proc = subprocess.Popen([full_bin_path])

        delay(0.5)

    def shutdown(self):
        if self.proc is not None:
            logging.info("shutting down broker")
            self.proc.kill()
            self.proc.wait(10)

        rdb_path = os.getcwd() + "/dump.rdb"

        if os.path.exists(rdb_path):
            os.remove(rdb_path)


@pytest.fixture(scope="function")
def redis_server(request):
    server = RedisServer()
    server.start()

    yield server

    server.shutdown()
