import asyncio
import time

from influxdb_client import WritePrecision

from Upper_Computer.settings import opcua_async_client, node_id, measurement, write_api, bucket, org, opcua_client


# from asyncua import Client
# from opcua import Client
#
# url = "opc.tcp://192.168.1.88:4840"
# node_id = "ns=4;s=变量表|count"
#
# client = Client(url)
# client.connect()


def get_vale():
    try:
        while True:
            start_time = time.time_ns()
            node = opcua_client.get_node(node_id)
            value = node.get_value()
            point = {
                "measurement": measurement,
                "tags": {"sensor": "汇川PLC"},
                "fields": {"count": value},
                "time": time.time_ns()
            }
            elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
            print(f"同步opc ua获取一次耗时{elapsed}毫秒，Value of {node_id}: {value}")
            write_api.write(bucket=bucket, org=org, record=point, write_precision=WritePrecision.NS)

    except Exception as e:
        print(e)
    finally:
        opcua_client.disconnect()


# 异步每次24ms 但是plc会返回too many sessions 拒绝请求
async def read_plc_data():
    async with opcua_async_client:
        while True:
            try:

                # await opcua_async_client.connect()
                # print("Connected to PLC!")
                start_time = time.time_ns()
                node = opcua_async_client.get_node(node_id)
                value = await node.read_value()
                point = {
                    "measurement": measurement,
                    "tags": {"sensor": "汇川PLC"},
                    "fields": {"count": value},
                    "time": time.time_ns()
                }
                elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
                print(f"Value of {node_id}: {value}")
                print(f"异步opc ua获取一次耗时{elapsed}毫秒")
                write_api.write(bucket=bucket, org=org, record=point, write_precision=WritePrecision.NS)

            except Exception as e:
                print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(read_plc_data())
    # get_vale()
