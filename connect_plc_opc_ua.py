import asyncio
import logging
import time
from datetime import datetime

from influxdb_client import WritePrecision
from opcua.tools import SubHandler

from Upper_Computer.settings import node_id, measurement, write_api, bucket, org, opcua_client, opcua_async_client, \
    logger

COUNT = 30000


# from asyncua import Client
# from opcua import Client
#
# url = "opc.tcp://192.168.1.88:4840"
# node_id = "ns=4;s=变量表|count"
#
# client = Client(url)
# client.connect()


class SubDataChangeHandler(SubHandler):
    def datachange_notification(self, node, val, data):
        output = f"{node.nodeid}: {val}"
        print(output)


def get_value():
    # opcua_client.connect()
    # nodes = opcua_client.get_node(node_id).get_children()
    #
    # logger.info(len(nodes))
    # logger.info(opcua_client.get_values(nodes))
    # for node in nodes:
    #     logger.info(node.get_browse_name())
    #     logger.info(type(node.get_browse_name()))
    #     logger.info(node.get_display_name())
    #     logger.info(type(node.get_display_name()))
    #     logger.info(node.get_data_value())
    #     logger.info(type(node.get_data_value()))
    #     logger.info(node.get_value())
    #     logger.info(type(node.get_value()))
    #     logger.info(node.get_properties())
    #     logger.info(type(node.get_properties()))
    #     logger.info(node.nodeid.NamespaceIndex)
    #     logger.info(node.nodeid.NodeIdType)
    #     logger.info(node.nodeid.NamespaceUri)
    #     logger.info(node.nodeid.ServerIndex)
    #     logger.info(node.nodeid.Identifier)
    #     logger.info(type(node.nodeid))
    #     logger.info(node.get_variables())
    #     logger.info(type(node.get_variables()))
    #     logger.info(node.get_value_rank())
    #     logger.info(type(node.get_value_rank()))
    #
    # opcua_client.disconnect()

    # 创建订阅监听数据变化
    opcua_client.connect()

    sub = opcua_client.create_subscription(0.1, SubDataChangeHandler())
    nodes = opcua_client.get_node(node_id).get_children()
    points = sub.subscribe_data_change(nodes, queuesize=1000)

    # start_time = time.time_ns()
    # try:
    #     while True:
    #         # start_time = time.time_ns()
    #         node = opcua_client.get_node(node_id)
    #         opcua_client.get_m
    #         value = node.get_value()
    #         points.append({datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"): value})
    #         # elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
    #         # logger.info(f"同步opc ua获取一次耗时{elapsed}毫秒，Value of {node_id}: {value}")
    #         # print(f"同步opc ua获取一次耗时{elapsed}毫秒，Value of {node_id}: {value}")
    #         # print(f"Value of {node_id}: {value}")
    #         # point = {
    #         #     "measurement": measurement,
    #         #     "tags": {"sensor": "汇川PLC"},
    #         #     "fields": {"count": value},
    #         #     "time": time.time_ns()
    #         # }
    #         # points.append(point)
    #         # if len(points) == 500:
    #         #     write_api.write(bucket=bucket, org=org, record=points, write_precision=WritePrecision.NS)
    #         #     points.clear()
    #         time.sleep(0.01)
    #         print(len(points))
    #         if len(points) == COUNT:
    #             print(f"耗时{round((time.time_ns() - start_time) / 1e6, 4)}毫秒")  # 纳秒换算成毫秒
    #             opcua_client.disconnect()
    #             break
    #         # points.append(point)
    #         # if len(points) == 250:
    #         #     # start_time = time.time_ns()
    #         #     write_api.write(bucket=bucket, org=org, record=points, write_precision=WritePrecision.NS)
    #         # elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
    #         # print(f"一次写入100条数据耗时{elapsed}毫秒")
    #         # logger.info(f"一次写入100条数据耗时{elapsed}毫秒")
    #         # points.clear()

    # except Exception as e:
    #     print(e)


# 异步每次24ms 但是plc会返回too many sessions 拒绝请求
async def read_plc_data():
    async with opcua_async_client:
        while True:
            try:
                await opcua_async_client.connect()
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
                print(f"异步opc ua获取一次耗时{elapsed}毫秒，Value of {node_id}: {value}")
                # write_api.write(bucket=bucket, org=org, record=point, write_precision=WritePrecision.NS)

            except Exception as e:
                print(f"Error: {e}")


if __name__ == "__main__":
    # res = []
    get_value()
    # while True:
    #     pass
    # retry_count = 0
    # max_retries = 10
    # res = []
    # while retry_count < max_retries:
    #     try:
    #         # time.sleep(0.1)
    #         opcua_client.connect()
    #         get_vale(res)
    #         if len(res) == COUNT:
    #             opcua_client.disconnect()
    #             break
    #         # await opcua_async_client.connect_socket()
    #         # asyncio.run(read_plc_data())
    #     except Exception as e:
    #         retry_count += 1
    #         # print()
    #         logger.error(f"第{retry_count}次尝试连接Error: {e}")
    # # 将列表元素转换为字符串并用换行符连接
    # data_str = '\n'.join(map(str, res))
    # with open('output.txt', 'w') as f:
    #     f.write(data_str)

    # with open("output_opc_ua.txt", "w") as f:
    #     for item in res:
    #         for key, value in item.items():
    #             f.write(f"{key},{value}\n")
