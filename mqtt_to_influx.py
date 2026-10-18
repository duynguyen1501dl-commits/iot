
import os
import json
import math
import time

from pathlib import Path
from collections import OrderedDict
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from influxdb_client import (
    InfluxDBClient,
    Point,
    WritePrecision
)

from influxdb_client.client.write_api import SYNCHRONOUS


load_dotenv(
    dotenv_path=Path(__file__).resolve().parent / ".env"
)

MQTT_HOST = os.getenv(
    "MQTT_HOST",
    "broker.hivemq.com"
)

MQTT_PORT = int(
    os.getenv("MQTT_PORT", "1883")
)

TOPIC_SENSOR = os.getenv(
    "MQTT_TOPIC",
    "iot/B23DCCN247/lab2/sensor"
)

TOPIC_BASE = TOPIC_SENSOR.rsplit("/", 1)[0]

TOPIC_ACK = TOPIC_BASE + "/ack"
TOPIC_LATENCY = TOPIC_BASE + "/latency"

INFLUX_URL = os.getenv(
    "INFLUX_URL",
    "http://localhost:8086"
)

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ptit")

INFLUX_BUCKET = os.getenv(
    "INFLUX_BUCKET_RAW",
    "iot_raw"
)

if not INFLUX_TOKEN:
    raise RuntimeError(
        "Chua co INFLUX_TOKEN trong .env"
    )


influx = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = influx.write_api(
    write_options=SYNCHRONOUS
)

seen = OrderedDict()
pending = OrderedDict()

MAX_HISTORY = 1000


def trim_history(store):
    while len(store) > MAX_HISTORY:
        store.popitem(last=False)


def validate_sensor(data):

    required = [
        "device_id",
        "seq",
        "ts_ms",
        "temperature",
        "humidity",
        "distance_cm",
        "rssi"
    ]

    if not isinstance(data, dict):
        raise ValueError("JSON khong phai object")

    for field in required:
        if field not in data:
            raise ValueError("Thieu truong " + field)

    device = str(data["device_id"])
    seq = int(data["seq"])
    ts_ms = int(data["ts_ms"])

    temperature = float(data["temperature"])
    humidity = float(data["humidity"])
    distance = float(data["distance_cm"])
    rssi = int(data["rssi"])

    if not device or len(device) > 64 or seq < 0:
        raise ValueError("device_id hoac seq sai")

    if not all(
        math.isfinite(x)
        for x in [temperature, humidity, distance]
    ):
        raise ValueError("Co gia tri NaN/Infinity")

    if not (-40 <= temperature <= 80):
        raise ValueError("Nhiet do khong hop le")

    if not (0 <= humidity <= 100):
        raise ValueError("Do am khong hop le")

    if distance != -1 and not (2 <= distance <= 400):
        raise ValueError("Khoang cach khong hop le")

    if not (-127 <= rssi <= 0):
        raise ValueError("RSSI khong hop le")

    if ts_ms < 1700000000000:
        raise ValueError("Timestamp chua hop le")

    return (
        device,
        seq,
        ts_ms,
        temperature,
        humidity,
        distance,
        rssi
    )


def handle_sensor(client, payload):

    data = json.loads(payload.decode("utf-8"))

    (
        device,
        seq,
        ts_ms,
        temperature,
        humidity,
        distance,
        rssi
    ) = validate_sensor(data)

    key = (device, seq, ts_ms)

    if key not in seen:

        point = (
            Point("sensor_raw")
            .tag("device_id", device)
            .field("seq", seq)
            .field("temperature", temperature)
            .field("humidity", humidity)
            .field("distance_cm", distance)
            .field("rssi", rssi)
            .time(ts_ms, WritePrecision.MS)
        )

        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=point
        )

        seen[key] = True
        trim_history(seen)

        print("\n-------------------------")
        print("[SENSOR] Da luu InfluxDB")
        print("Device:", device)
        print("Seq:", seq)
        print("Nhiet do:", temperature, "C")
        print("Do am:", humidity, "%")
        print("Khoang cach:", distance, "cm")

    else:
        print("[SENSOR] Du lieu trung:", seq)

    pending[(device, seq)] = (
        ts_ms,
        time.monotonic()
    )

    trim_history(pending)

    info = client.publish(
        TOPIC_ACK,
        str(seq),
        qos=0,
        retain=False
    )

    if info.rc == mqtt.MQTT_ERR_SUCCESS:
        print("[ACK] Da xep gui seq:", seq)
    else:
        pending.pop((device, seq), None)
        print("[ACK] Gui that bai:", info.rc)

    pc_time_ms = int(time.time() * 1000)
    clock_difference = pc_time_ms - ts_ms

    if abs(clock_difference) > 60000:
        print(
            "[WARNING] Chenh lech dong ho:",
            clock_difference,
            "ms"
        )


def handle_latency(payload):

    data = json.loads(payload.decode("utf-8"))

    if not isinstance(data, dict):
        raise ValueError("Latency JSON khong hop le")

    device = str(data["device_id"])
    seq = int(data["seq"])
    rtt_ms = float(data["rtt_ms"])

    if not device or len(device) > 64 or seq < 0:
        raise ValueError("device_id hoac seq sai")

    if not math.isfinite(rtt_ms):
        raise ValueError("RTT khong phai so hop le")

    if not (0 <= rtt_ms <= 120000):
        raise ValueError("RTT khong hop le")

    key = (device, seq)
    matched = pending.pop(key, None)

    if matched is None:
        print(
            "[RTT] Khong tim thay goi dang cho:",
            seq
        )
        return

    ts_ms, created_at = matched

    if time.monotonic() - created_at > 120:
        print("[RTT] ACK qua cu:", seq)
        return

    point_latency = (
        Point("sensor_latency")
        .tag("device_id", device)
        .field("seq", seq)
        .field("rtt_ms", rtt_ms)
        .time(
            datetime.now(timezone.utc),
            WritePrecision.MS
        )
    )

    point_raw = (
        Point("sensor_raw")
        .tag("device_id", device)
        .field("rtt_ms", rtt_ms)
        .time(ts_ms, WritePrecision.MS)
    )

    write_api.write(
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
        record=[
            point_latency,
            point_raw
        ]
    )

    print("\n-------------------------")
    print("[RTT] Da luu InfluxDB")
    print("Device:", device)
    print("Seq:", seq)
    print("RTT:", rtt_ms, "ms")


def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("[MQTT] Ket noi thanh cong")

        client.subscribe([
            (TOPIC_SENSOR, 0),
            (TOPIC_LATENCY, 0)
        ])

        print("[SUB]", TOPIC_SENSOR)
        print("[SUB]", TOPIC_LATENCY)
        print("[PUB ACK]", TOPIC_ACK)

    else:
        print("[MQTT] Loi ket noi:", rc)


def on_message(client, userdata, msg):

    try:
        if msg.topic == TOPIC_SENSOR:
            handle_sensor(
                client,
                msg.payload
            )

        elif msg.topic == TOPIC_LATENCY:
            handle_latency(
                msg.payload
            )

    except Exception as error:
        print(
            "[ERROR]",
            msg.topic,
            str(error)
        )


def main():

    mqtt_client = mqtt.Client(
        client_id="ptit-python-" + str(os.getpid())
    )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    print("===== IOT LAB 2 =====")
    print("MQTT:", MQTT_HOST)
    print("InfluxDB:", INFLUX_URL)
    print("Bucket:", INFLUX_BUCKET)

    try:
        mqtt_client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=60
        )

        mqtt_client.loop_forever()

    except KeyboardInterrupt:
        print("\nDa dung MQTT Collector")

    finally:
        mqtt_client.disconnect()
        write_api.close()
        influx.close()


if __name__ == "__main__":
    main()
