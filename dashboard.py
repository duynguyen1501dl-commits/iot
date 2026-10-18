
import os
import json
import math

from pathlib import Path
from flask import Flask, jsonify, render_template_string
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv(
    dotenv_path=Path(__file__).resolve().parent / ".env"
)

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

DEVICE_ID = "esp32-01"


def read_series(client, bucket, measurement, fields):

    result = {field: [] for field in fields}

    field_filter = " or ".join(
        'r._field == {}'.format(json.dumps(field))
        for field in fields
    )

    query = '''
    from(bucket: {})
      |> range(start: -6h)
      |> filter(fn: (r) => r._measurement == {})
      |> filter(fn: (r) => r.device_id == {})
      |> filter(fn: (r) => {})
      |> aggregateWindow(
          every: 15s,
          fn: last,
          createEmpty: false
      )
    '''.format(
        json.dumps(bucket),
        json.dumps(measurement),
        json.dumps(DEVICE_ID),
        field_filter
    )

    tables = client.query_api().query(
        query,
        org=INFLUX_ORG
    )

    for table in tables:
        for record in table.records:

            field = record.get_field()
            value = record.get_value()

            if field not in result or value is None:
                continue

            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            if not math.isfinite(value):
                continue

            result[field].append({
                "time": record.get_time().isoformat(),
                "value": value
            })

    for field in fields:
        result[field].sort(
            key=lambda item: item["time"]
        )

    return result


@app.route("/api/data")
def api_data():

    try:
        with InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG
        ) as client:

            raw = read_series(
                client,
                RAW_BUCKET,
                "sensor_raw",
                [
                    "temperature",
                    "humidity",
                    "distance_cm",
                    "rtt_ms"
                ]
            )

            latency = read_series(
                client,
                RAW_BUCKET,
                "sensor_latency",
                ["rtt_ms"]
            )

            processed = {
                "temperature_roll3": [],
                "temperature_z": []
            }

            processed_error = None

            try:
                processed = read_series(
                    client,
                    PROCESSED_BUCKET,
                    "sensor_processed",
                    [
                        "temperature_roll3",
                        "temperature_z"
                    ]
                )
            except Exception as error:
                processed_error = str(error)

        return jsonify({
            "raw": raw,
            "latency": latency,
            "processed": processed,
            "processed_error": processed_error
        })

    except Exception as error:
        return jsonify({
            "error": str(error)
        }), 500


HTML = """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>PTIT IoT Dashboard - ACK RTT</title>

<style>
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    font-family: Arial, sans-serif;
    background: #f2f5fa;
    color: #172944;
}

header {
    background: #193f76;
    padding: 24px 35px;
    color: white;
}

header h1 {
    margin: 0 0 8px;
}

header p {
    margin: 0;
    color: #d7e5fa;
}

main {
    max-width: 1250px;
    margin: auto;
    padding: 22px;
}

#status {
    margin: 5px 0 18px;
    font-size: 14px;
}

.metrics {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(190px, 1fr));
    gap: 14px;
}

.card {
    background: white;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 2px 9px #00000010;
}

.card h3 {
    margin: 0 0 13px;
    color: #50627e;
    font-size: 15px;
}

.value {
    font-size: 27px;
    font-weight: bold;
    overflow-wrap: anywhere;
}

.note {
    color: #65758d;
    font-size: 12px;
    margin-top: 8px;
}

.charts {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(330px, 1fr));
    gap: 16px;
    margin-top: 20px;
}

.chart svg {
    width: 100%;
    height: 230px;
}

#details {
    margin-top: 20px;
    color: #52637d;
    line-height: 1.8;
}

footer {
    text-align: center;
    color: #667;
    padding: 20px;
}

@media (max-width: 500px) {
    main { padding: 12px; }
    header { padding: 20px; }
    .charts { grid-template-columns: 1fr; }
}
</style>
</head>

<body>

<header>
    <h1>PTIT - IoT Dashboard</h1>
    <p>ESP32 | HiveMQ | InfluxDB | Flask | ACK & RTT</p>
</header>

<main>

<div id="status">Đang tải dữ liệu...</div>

<div class="metrics">

  <div class="card">
    <h3>Nhiệt độ</h3>
    <div class="value" id="temp">--</div>
  </div>

  <div class="card">
    <h3>Độ ẩm</h3>
    <div class="value" id="hum">--</div>
  </div>

  <div class="card">
    <h3>Khoảng cách</h3>
    <div class="value" id="dist">--</div>
  </div>

  <div class="card">
    <h3>RTT mới nhất</h3>
    <div class="value" id="rtt">--</div>
    <div class="note" id="rttNote">
      Chưa nhận dữ liệu ACK/RTT
    </div>
  </div>

  <div class="card">
    <h3>RTT trung bình</h3>
    <div class="value" id="rttAvg">--</div>
    <div class="note">Theo các điểm hiển thị trong 6 giờ</div>
  </div>

</div>

<div class="charts">

  <div class="card chart">
    <h3>Biểu đồ nhiệt độ (°C)</h3>
    <svg id="tempChart" viewBox="0 0 700 230"></svg>
  </div>

  <div class="card chart">
    <h3>Biểu đồ độ ẩm (%)</h3>
    <svg id="humChart" viewBox="0 0 700 230"></svg>
  </div>

  <div class="card chart">
    <h3>Biểu đồ khoảng cách (cm)</h3>
    <svg id="distChart" viewBox="0 0 700 230"></svg>
  </div>

  <div class="card chart">
    <h3>Biểu đồ RTT (ms)</h3>
    <svg id="rttChart" viewBox="0 0 700 230"></svg>
  </div>

  <div class="card chart">
    <h3>Nhiệt độ trung bình trượt (°C)</h3>
    <svg id="processedChart" viewBox="0 0 700 230"></svg>
  </div>

</div>

<div id="details"></div>

</main>

<footer>
    Dashboard tự cập nhật mỗi 5 giây
</footer>

<script>

function lastItem(series) {
    if (!series || series.length === 0) {
        return null;
    }

    return series[series.length - 1];
}

function showMetric(id, value, unit) {
    const element = document.getElementById(id);

    if (value !== null && Number.isFinite(value)) {
        element.textContent = value.toFixed(2) + " " + unit;
    } else {
        element.textContent = "--";
    }
}

function drawChart(id, series, color, hideNegative=false) {

    const svg = document.getElementById(id);

    const W = 700;
    const H = 230;
    const left = 55;
    const right = 18;
    const top = 18;
    const bottom = 35;

    const points = (series || [])
        .filter(item => Number.isFinite(item.value))
        .filter(item => !hideNegative || item.value >= 0)
        .map(item => ({
            t: new Date(item.time).getTime(),
            v: item.value
        }))
        .filter(item => Number.isFinite(item.t));

    if (points.length === 0) {
        svg.innerHTML =
            '<text x="230" y="110" fill="#64748b">' +
            'Chưa có dữ liệu</text>';
        return;
    }

    let xmin = Math.min(...points.map(p => p.t));
    let xmax = Math.max(...points.map(p => p.t));
    let ymin = Math.min(...points.map(p => p.v));
    let ymax = Math.max(...points.map(p => p.v));

    if (xmin === xmax) {
        xmax = xmin + 1000;
    }

    if (ymin === ymax) {
        ymin -= 1;
        ymax += 1;
    }

    const x = t => left +
        (t - xmin) / (xmax - xmin) *
        (W - left - right);

    const y = v => H - bottom -
        (v - ymin) / (ymax - ymin) *
        (H - top - bottom);

    let html = "";

    for (let i = 0; i <= 4; i++) {
        const val = ymin + (ymax - ymin) * i / 4;
        const yy = y(val);

        html += `<line x1="${left}" y1="${yy}"
          x2="${W-right}" y2="${yy}"
          stroke="#e2e8f0" />`;

        html += `<text x="3" y="${yy+4}"
          font-size="12" fill="#64748b">
          ${val.toFixed(1)}</text>`;
    }

    const coords = points.map(
        p => `${x(p.t)},${y(p.v)}`
    ).join(" ");

    html += `<polyline points="${coords}"
      fill="none" stroke="${color}"
      stroke-width="3" stroke-linejoin="round" />`;

    for (const p of points) {
        html += `<circle cx="${x(p.t)}"
          cy="${y(p.v)}" r="3"
          fill="${color}" />`;
    }

    const first = new Date(points[0].t)
        .toLocaleTimeString("vi-VN");

    const last = new Date(points[points.length - 1].t)
        .toLocaleTimeString("vi-VN");

    html += `<text x="${left}" y="222"
      font-size="12" fill="#64748b">${first}</text>`;

    html += `<text x="${W-115}" y="222"
      font-size="12" fill="#64748b">${last}</text>`;

    svg.innerHTML = html;
}

let busy = false;

async function updateDashboard() {

    if (busy) return;
    busy = true;

    const status = document.getElementById("status");

    try {
        const response = await fetch(
            "/api/data",
            { cache: "no-store" }
        );

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Lỗi API");
        }

        const raw = data.raw;
        const processed = data.processed;

        const tempSeries = raw.temperature || [];
        const humSeries = raw.humidity || [];
        const distSeries = raw.distance_cm || [];

        const latencySeries = data.latency.rtt_ms || [];
        const rawRttSeries = raw.rtt_ms || [];

        // Uu tien RTT doc tu measurement sensor_latency
        const rttSeries = latencySeries.length > 0
            ? latencySeries
            : rawRttSeries;

        const temp = lastItem(tempSeries);
        const hum = lastItem(humSeries);
        const dist = lastItem(distSeries);
        const rtt = lastItem(rttSeries);

        showMetric(
            "temp",
            temp ? temp.value : null,
            "°C"
        );

        showMetric(
            "hum",
            hum ? hum.value : null,
            "%"
        );

        if (dist && dist.value < 0) {
            document.getElementById("dist").textContent =
                "Không có echo";
        } else {
            showMetric(
                "dist",
                dist ? dist.value : null,
                "cm"
            );
        }

        showMetric(
            "rtt",
            rtt ? rtt.value : null,
            "ms"
        );

        const rttNote = document.getElementById("rttNote");

        if (rtt) {
            const age = (
                Date.now() - new Date(rtt.time).getTime()
            ) / 1000;

            if (age > 60) {
                rttNote.textContent =
                    "Cảnh báo: RTT cuối đã cũ " +
                    Math.round(age) + " giây";
            } else {
                rttNote.textContent =
                    "Đã nhận RTT từ ACK";
            }
        } else {
            rttNote.textContent =
                "Chưa có rtt_ms trong InfluxDB";
        }

        const values = rttSeries
            .map(item => item.value)
            .filter(Number.isFinite);

        let average = null;

        if (values.length > 0) {
            average = values.reduce(
                (a, b) => a + b, 0
            ) / values.length;
        }

        showMetric("rttAvg", average, "ms");

        drawChart(
            "tempChart",
            tempSeries,
            "#e76f51"
        );

        drawChart(
            "humChart",
            humSeries,
            "#277da1"
        );

        drawChart(
            "distChart",
            distSeries,
            "#43aa8b",
            true
        );

        drawChart(
            "rttChart",
            rttSeries,
            "#8e44ad"
        );

        drawChart(
            "processedChart",
            processed.temperature_roll3 || [],
            "#f4a261"
        );

        const detail = document.getElementById("details");

        detail.textContent =
            "Số điểm nhiệt độ: " + tempSeries.length +
            " | Số điểm RTT: " + rttSeries.length +
            " | Số điểm đã xử lý: " +
            (processed.temperature_roll3 || []).length;

        const lastSensor = temp || hum || dist;

        if (lastSensor) {
            status.textContent =
                "Đã kết nối InfluxDB | Dữ liệu cảm biến: " +
                new Date(lastSensor.time)
                    .toLocaleString("vi-VN");
        } else {
            status.textContent =
                "Đã kết nối InfluxDB nhưng không có " +
                "dữ liệu cảm biến trong 6 giờ";
        }

        if (data.processed_error) {
            detail.textContent +=
                " | Lỗi dữ liệu xử lý: " +
                data.processed_error;
        }

    } catch (error) {
        status.textContent =
            "Lỗi kết nối/truy vấn: " + error.message;
    } finally {
        busy = false;
    }
}

updateDashboard();
setInterval(updateDashboard, 5000);

</script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


if __name__ == "__main__":
    print("===== PTIT IOT DASHBOARD =====")
    print("InfluxDB:", INFLUX_URL)
    print("Raw bucket:", RAW_BUCKET)
    print("Processed bucket:", PROCESSED_BUCKET)
    print("Dashboard: http://127.0.0.1:5000")

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False
    )
