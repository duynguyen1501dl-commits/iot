
import os
import time

import numpy as np
import pandas as pd

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

URL = os.getenv("INFLUX_URL")
ORG = os.getenv("INFLUX_ORG")
TOKEN = os.environ["INFLUX_TOKEN"]

RAW = os.getenv("INFLUX_BUCKET_RAW")
PROCESSED = os.getenv("INFLUX_BUCKET_PROCESSED")

COLUMNS = [
    "temperature",
    "humidity",
    "distance_cm"
]


def run_once(client):

    query = '''
    from(bucket: "{}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "sensor_raw")
      |> filter(fn: (r) =>
          r._field == "temperature" or
          r._field == "humidity" or
          r._field == "distance_cm"
      )
      |> filter(fn: (r) => r.device_id == "esp32-01")
    '''.format(RAW)

    rows = []

    tables = client.query_api().query(query, org=ORG)

    for table in tables:
        for record in table.records:
            rows.append({
                "time": record.get_time(),
                "field": record.get_field(),
                "value": record.get_value()
            })

    if not rows:
        print("[PROCESS] Chua co du lieu")
        return

    raw = pd.DataFrame(rows)

    data = raw.pivot_table(
        index="time",
        columns="field",
        values="value",
        aggfunc="last"
    )

    data = data.reindex(columns=COLUMNS).sort_index()

    data["distance_cm"] = data["distance_cm"].replace(
        -1, np.nan
    )

    outliers = 0

    for col in COLUMNS:
        s = data[col].dropna()

        if len(s) >= 8:
            q1 = s.quantile(0.25)
            q3 = s.quantile(0.75)

            iqr = q3 - q1

            if iqr > 0:
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr

                mask = (
                    (data[col] < lower) |
                    (data[col] > upper)
                )

                outliers += int(mask.sum())
                data.loc[mask, col] = np.nan

    data[COLUMNS] = data[COLUMNS].interpolate(
        method="time",
        limit=3
    )

    minute = data.resample("1min").mean()

    minute = minute.interpolate(limit=2)

    minute = minute.dropna(
        subset=["temperature", "humidity"],
        how="all"
    )

    if minute.empty:
        print("[PROCESS] Khong con du lieu hop le")
        return

    minute["temperature_roll3"] = (
        minute["temperature"]
        .rolling(3, min_periods=1)
        .mean()
    )

    minute["temperature_delta"] = (
        minute["temperature"]
        .diff()
        .fillna(0)
    )

    minute["temperature_z"] = np.nan

    valid = minute["temperature"].dropna()

    if len(valid) > 0:
        mean = valid.mean()
        std = valid.std(ddof=0)

        if std > 0:
            minute.loc[valid.index, "temperature_z"] = (
                (valid - mean) / std
            )
        else:
            minute.loc[valid.index, "temperature_z"] = 0.0

    points = []

    for timestamp, item in minute.iterrows():

        point = (
            Point("sensor_processed")
            .tag("device_id", "esp32-01")
            .time(
                timestamp.to_pydatetime(),
                WritePrecision.MS
            )
        )

        count = 0

        fields = COLUMNS + [
            "temperature_roll3",
            "temperature_delta",
            "temperature_z"
        ]

        for col in fields:
            val = item[col]

            if pd.notna(val) and np.isfinite(val):
                point.field(col, float(val))
                count += 1

        if count:
            points.append(point)

    client.write_api(
        write_options=SYNCHRONOUS
    ).write(
        bucket=PROCESSED,
        org=ORG,
        record=points
    )

    print(
        "[PROCESS] Raw={} Minutes={} Outliers={} Saved={}".format(
            len(data),
            len(minute),
            outliers,
            len(points)
        )
    )


def main():
    with InfluxDBClient(
        url=URL,
        token=TOKEN,
        org=ORG
    ) as client:

        while True:
            try:
                run_once(client)
            except Exception as e:
                print("[ERROR]", e)

            time.sleep(60)


if __name__ == "__main__":
    main()
