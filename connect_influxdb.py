import random
import time
from datetime import timezone, timedelta, datetime

import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

token = "kDKf6ACTpykjjlX-4TsgmpwcU1MAae7AY6wM91-10wv2UxDPlnY2qZyVfmDT5ld0ytD_w0IC4cRxVn4RuhzFzQ=="
org = "my-org"
url = "http://localhost:8086"
bucket = "my-bucket"
measurement = "experiment"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

write_api = client.write_api(write_options=ASYNCHRONOUS)
points = []
# now_ns = int((datetime.now() - timedelta(minutes=10)).timestamp() * 1e9)  # 微秒级时间戳乘以1e6 到纳秒级就乘以1e9
for value in range(500000):
    print(value)
    # 微秒级时间戳乘以1e6 到纳秒级就乘以1e9
    now_ns = int((datetime.now() - timedelta(minutes=3, microseconds=-value)).timestamp() * 1e9)
    # point = Point(measurement).tag("sensor", "sensor1").field("temperature", value).time(
    #     now_ns, write_precision=WritePrecision.NS)
    # temp = random.randint(0, 1000)
    point = {
        "measurement": measurement,
        "tags": {"sensor": "2号传感器"},
        "fields": {"temperature": value, "转数": value/2},
        "time": now_ns
    }
    print(point)
    points.append(point)
# print(points)
try:
    write_api.write(bucket=bucket, org=org, record=points, write_precision=WritePrecision.NS)
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
