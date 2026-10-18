
import os
import json
import math

from pathlib import Path
from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

app = Flask(__name__)

INFLUX_URL = os.getenv(
    "INFLUX_URL", "http://localhost:8086"
)
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ptit")

RAW_BUCKET = os.getenv(
    "INFLUX_BUCKET_RAW", "iot_raw"
)
PROCESSED_BUCKET = os.getenv(
    "INFLUX_BUCKET_PROCESSED", "iot_processed"
)

DEVICE_ID = os.getenv("DEVICE_ID", "esp32-01")

TIME_RANGES = {
    "15m": ("15m", "5s"),
    "1h": ("1h", "15s"),
    "6h": ("6h", "1m"),
    "24h": ("24h", "5m"),
    "7d": ("7d", "30m")
}

METRICS = {
    "temperature": ("sensor_raw", "temperature"),
    "humidity": ("sensor_raw", "humidity"),
    "distance_cm": ("sensor_raw", "distance_cm"),
    "rtt_ms": ("sensor_latency", "rtt_ms"),
    "temperature_roll3": (
        "sensor_processed", "temperature_roll3"
    ),
    "temperature_z": (
        "sensor_processed", "temperature_z"
    )
}

DEFAULT_METRICS = [
    "temperature",
    "humidity",
    "distance_cm",
    "rtt_ms"
]


def get_bucket(measurement):
    if measurement == "sensor_processed":
        return PROCESSED_BUCKET
    return RAW_BUCKET


def query_measurement(
    client, measurement, fields,
    duration, interval, aggregation
):
    bucket = get_bucket(measurement)

    field_filter = " or ".join(
        "r._field == {}".format(json.dumps(field))
        for field in fields
    )

    flux = '''
    from(bucket: {})
      |> range(start: -{})
      |> filter(fn: (r) => r._measurement == {})
      |> filter(fn: (r) => r.device_id == {})
      |> filter(fn: (r) => {})
      |> aggregateWindow(
          every: {},
          fn: {},
          createEmpty: false
      )
    '''.format(
        json.dumps(bucket),
        duration,
        json.dumps(measurement),
        json.dumps(DEVICE_ID),
        field_filter,
        interval,
        aggregation
    )

    series = {field: [] for field in fields}

    tables = client.query_api().query(
        flux,
        org=INFLUX_ORG
    )

    for table in tables:
        for record in table.records:
            field = record.get_field()
            value = record.get_value()

            if field not in series or value is None:
                continue

            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            if not math.isfinite(value):
                continue

            series[field].append({
                "time": record.get_time().isoformat(),
                "value": value
            })

    for field in fields:
        series[field].sort(
            key=lambda point: point["time"]
        )

    return series


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    if not INFLUX_TOKEN:
        return jsonify({
            "error": "Thieu INFLUX_TOKEN trong file .env"
        }), 500

    range_key = request.args.get("range", "1h")
    aggregation = request.args.get("agg", "last")

    if range_key not in TIME_RANGES:
        return jsonify({
            "error": "Khoang thoi gian khong hop le"
        }), 400

    if aggregation not in ("last", "mean"):
        return jsonify({
            "error": "Kieu tong hop khong hop le"
        }), 400

    requested = request.args.getlist("metric")

    if not requested:
        requested = DEFAULT_METRICS

    selected = list(dict.fromkeys(
        metric for metric in requested
        if metric in METRICS
    ))

    duration, interval = TIME_RANGES[range_key]

    series = {metric: [] for metric in selected}
    warnings = []

    groups = {}

    for metric in selected:
        measurement, field = METRICS[metric]
        groups.setdefault(measurement, []).append(field)

    try:
        with InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        ) as client:

            for measurement, fields in groups.items():
                try:
                    result = query_measurement(
                        client,
                        measurement,
                        fields,
                        duration,
                        interval,
                        aggregation
                    )

                    for field, points in result.items():
                        series[field] = points

                except Exception as error:
                    warnings.append(
                        "{}: {}".format(
                            measurement, str(error)
                        )
                    )

            if "rtt_ms" in selected and not series["rtt_ms"]:
                try:
                    fallback = query_measurement(
                        client,
                        "sensor_raw",
                        ["rtt_ms"],
                        duration,
                        interval,
                        aggregation
                    )

                    series["rtt_ms"] = fallback["rtt_ms"]

                except Exception:
                    pass

        return jsonify({
            "device": DEVICE_ID,
            "range": range_key,
            "aggregation": aggregation,
            "series": series,
            "warnings": warnings
        })

    except Exception as error:
        return jsonify({
            "error": "Khong truy van duoc InfluxDB: {}".format(
                error
            )
        }), 500


if __name__ == "__main__":
    print("===== IOT WEB MONITOR =====")
    print("Device:", DEVICE_ID)
    print("InfluxDB:", INFLUX_URL)
    print("Web: http://127.0.0.1:5001")

    app.run(
        host="127.0.0.1",
        port=5001,
        debug=False
    )
