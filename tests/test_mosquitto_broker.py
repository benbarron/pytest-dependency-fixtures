import time
import logging

import paho.mqtt.client as paho_mqtt
import paho.mqtt.enums as paho_types

from pytest_dependency_fixtures import MosquittoBroker, mosquitto_broker
from pytest_dependency_fixtures.utils import delay


def _wait_for_connection(client: paho_mqtt.Client, timeout_ms: int):
    timeout_step_ms = 50
    timeout_total = timeout_ms

    while timeout_total > 0:
        if client.is_connected():
            return
        timeout_total - timeout_step_ms
        time.sleep(timeout_step_ms / 1e3)

    raise ConnectionError(f"failed to connect before timeout: {timeout_ms} [ms]")


def test_mosquitto_broker_connection(mosquitto_broker: MosquittoBroker):
    paho_client = paho_mqtt.Client(
        callback_api_version=paho_types.CallbackAPIVersion.VERSION2,
        protocol=paho_types.MQTTProtocolVersion.MQTTv311,
        transport="tcp",
        client_id="test-conn",
        reconnect_on_failure=True,
        manual_ack=False,
    )

    def on_connect(client, userdata, flags, reason_code, properties):
        logging.info("connected")
        assert reason_code == 0

    paho_client.on_connect = on_connect

    paho_client.enable_logger()
    paho_client.loop_start()
    paho_client.connect("127.0.0.1", port=1883)

    _wait_for_connection(paho_client, 3000)

    assert paho_client.is_connected()
    paho_client.loop_stop()
    paho_client.disconnect()


def test_mosquitto_broker_pubsub(mosquitto_broker: MosquittoBroker):
    paho_client = paho_mqtt.Client(
        callback_api_version=paho_types.CallbackAPIVersion.VERSION2,
        protocol=paho_types.MQTTProtocolVersion.MQTTv311,
        transport="tcp",
        client_id="test-pubsub",
        reconnect_on_failure=True,
        manual_ack=False,
    )

    sent_msgs = ["testing msg 1", "testing msg 2"]

    received_msgs = []

    def on_connect(client, userdata, flags, reason_code, properties):
        logging.info("connected")
        assert reason_code == 0

    def on_message(client, userdata, msg):
        received_msgs.append(msg)

    paho_client.on_connect = on_connect
    paho_client.on_message = on_message

    paho_client.enable_logger()
    paho_client.loop_start()
    paho_client.connect("127.0.0.1", port=1883)

    _wait_for_connection(paho_client, 3000)

    assert paho_client.is_connected()

    paho_client.subscribe("test", qos=2)

    msg_info: paho_mqtt.MQTTMessageInfo = paho_client.publish(
        topic="test",
        payload=sent_msgs[0],
        qos=2,
    )
    msg_info.wait_for_publish(timeout=10)

    delay(0.5)

    msg_info: paho_mqtt.MQTTMessageInfo = paho_client.publish(
        topic="test",
        payload=sent_msgs[1],
        qos=2,
    )
    msg_info.wait_for_publish(timeout=10)

    delay(0.5)

    assert len(received_msgs) == 2
    assert received_msgs[0].payload.decode() == sent_msgs[0]
    assert received_msgs[1].payload.decode() == sent_msgs[1]

    paho_client.loop_stop()
    paho_client.disconnect()
