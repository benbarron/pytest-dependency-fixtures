import os
import time
import logging
import pytest
import platform
import subprocess
import typing as T

from .utils import delay


class MosquittoBroker:

    def __init__(self) -> None:
        self.proc: T.Optional[subprocess.Popen] = None

    def start(self):
        uname = platform.uname()
        bin_name = f"mosquitto_{uname.system}_{uname.machine}"

        full_bin_path = os.getcwd() + "/3rdparty/mosquitto/" + bin_name
        config_path = os.getcwd() + "/3rdparty/mosquitto/mosquitto.conf"

        logging.info(f"starting broker: {full_bin_path} {config_path}")
        self.proc = subprocess.Popen([full_bin_path, "-c", config_path])

        delay(0.5)

    def shutdown(self):
        if self.proc is not None:
            logging.info("shutting down broker")
            self.proc.kill()
            self.proc.wait(10)


@pytest.fixture(scope="function")
def mosquitto_broker(request):
    broker = MosquittoBroker()
    broker.start()

    yield broker

    broker.shutdown()
