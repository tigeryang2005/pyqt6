import random
import time
from datetime import timezone, timedelta, datetime

import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

token = "d3VP9A48seA-pnvdr7O-q4GGYJ2sFS1xU5ZU2iLHmVGupzydHeNGff3tPCxiUVMIDH3XPowYf99S3fGgX33wBA=="
org = "ada"
url = "http://localhost:8086"
bucket = "init1"
measurement = "measurement"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

write_api = client.write_api(write_options=ASYNCHRONOUS)
points = []
# now_ns = int((datetime.now() - timedelta(minutes=10)).timestamp() * 1e9)  # 微秒级时间戳乘以1e6 到纳秒级就乘以1e9
for value in range(10):
    print(value)
    now_ns = int((datetime.now() - timedelta(minutes=6, microseconds=1)).timestamp() * 1e9)  # 微秒级时间戳乘以1e6 到纳秒级就乘以1e9
    # point = Point(measurement).tag("sensor", "sensor1").field("temperature", value).time(
    #     now_ns, write_precision=WritePrecision.NS)
    temp = random.randint(0, 10)
    point = {
        "measurement": measurement,
        "tags": {"sensor": "2号传感器"},
        "fields": {"temperature": temp},
        "time": now_ns
    }
    print(point)
    points.append(point)
# print(points)
try:
    write_api.write(bucket=bucket, org="ada", record=points, write_precision=WritePrecision.NS)
except Exception as e:
    print(e)
# time.sleep(1)  # separate points by 1 second
write_api.close()

# query_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

query = f"""from(bucket: "{bucket}")
 |> range(start: -7m)
 |> filter(fn: (r) => r._measurement == "{measurement}")
 """

print(query)
tables = query_api.query(query=query, org=org)

for table in tables:
    print(len(table.records))
    # for t in table.records:
    #     print(t)
client.close()
