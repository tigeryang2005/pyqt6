from opcua import Client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from asyncua import Client as AsyncClient
# opcua同步连接plc
plc_opcua_url = "opc.tcp://192.168.1.88:4840"
node_id = "ns=4;s=变量表|count"

opcua_client = Client(plc_opcua_url)
opcua_client.connect()
# opcua异步连接plc
opcua_async_client = AsyncClient(plc_opcua_url)

token = "kDKf6ACTpykjjlX-4TsgmpwcU1MAae7AY6wM91-10wv2UxDPlnY2qZyVfmDT5ld0ytD_w0IC4cRxVn4RuhzFzQ=="
org = "my-org"
url = "http://localhost:8086"
bucket = "my-bucket"
measurement = "experiment"

influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)
