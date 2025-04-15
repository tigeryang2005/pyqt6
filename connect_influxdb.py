from datetime import timezone, timedelta, datetime

import influxdb_client
import time
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryOptions


token = "d3VP9A48seA-pnvdr7O-q4GGYJ2sFS1xU5ZU2iLHmVGupzydHeNGff3tPCxiUVMIDH3XPowYf99S3fGgX33wBA=="
org = "ada"
url = "http://localhost:8086"
bucket = "initdb"
measurement = "measurement1"


# 生成中国时区时间戳（UTC+8）
tz_shanghai = timezone(timedelta(hours=8))


client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

write_api = client.write_api(write_options=SYNCHRONOUS)
points = []
for value in range(5):
    now_shanghai_ns = int(datetime.now(tz_shanghai).timestamp() * 1e6)  # 微秒级时间戳 到纳秒级就乘以1e9
    print(now_shanghai_ns)
    print(int(datetime.now().timestamp() * 1e6))
    point = (
        Point(measurement)
        .tag("tagname1", "tagvalue1")
        .field("field1", value)
        .time(now_shanghai_ns, write_precision=WritePrecision.NS)
    )
    points.append(point)
print(points)
write_api.write(bucket=bucket, org="ada", record=points)
time.sleep(1)  # separate points by 1 second

query_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

query = f"""from(bucket: "{bucket}")
 |> range(start: -1h)
 |> filter(fn: (r) => r._measurement == "{measurement}")
 |> timezone(name: "Asia/Shanghai")
 """

print(query)
tables = query_api.query(query=query, org=org)

for table in tables:
    for t in table.records:
        print(t)
client.close()
