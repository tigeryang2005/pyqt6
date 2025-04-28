import asyncio
import logging
import time
from datetime import datetime

from influxdb_client import WritePrecision
from opcua.tools import SubHandler

from Upper_Computer.settings import node_id, measurement, write_api, bucket, org, opcua_client, opcua_async_client, \
    logger, COUNT


class SubDataChangeHandler(SubHandler):
    def datachange_notification(self, node, val, data):
        output = f"{node.nodeid}: {val}"
        logging.info(output)


def get_value_subdata_change():
    # 创建订阅监听数据变化
    opcua_client.connect()

    sub = opcua_client.create_subscription(0.1, SubDataChangeHandler())
    nodes = opcua_client.get_node(node_id).get_children()
    points = sub.subscribe_data_change(nodes, queuesize=1000)


def get_values_sync_client():
    init_time = time.time_ns()
    error_times = 0
    points = []
    max_retry_times = 10
    opcua_client.connect()

    while True and error_times < max_retry_times:
        try:
            start_time = time.time_ns()
            nodes = opcua_client.get_node(node_id).get_children()
            values = opcua_client.get_values(nodes)
            end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
            result_tostring = ','.join(map(str, values))
            point = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}: 每次获取耗时{end_time}毫秒，当前获取个数:{len(points)} 值：{values}"
            points.append(point)
            logger.info(point)
            if len(points) == COUNT:
                total_time = time.time_ns() - round(time.time_ns() - init_time, 4) / 1e6  # 毫秒
                result = f"{COUNT}次连接共耗时{total_time}毫秒即{round(total_time / 60000, 4)}分,平均连接一次耗时{round(total_time / COUNT, 4)}毫秒"
                logger.info(result)
                points.append(result)
                points.append(f"总共超时次数：{error_times}\n")
                with open("output_opc_ua.txt", "a", encoding='utf-8') as f:
                    f.write('\n'.join(map(str, points)))
                break
        except Exception as e:
            logger.error(e)
            error_times += 1
        finally:
            opcua_client.disconnect()


# 异步每次24ms 但是plc会返回too many sessions 拒绝请求
async def get_values_async_client():
    init_time = time.time_ns()
    error_times = 0
    points = []
    max_retry_times = 10
    while True and error_times < max_retry_times:
        try:
            await opcua_async_client.connect()
            start_time = time.time_ns()
            nodes = await opcua_async_client.get_node(node_id).get_children()
            values = await opcua_async_client.read_values(nodes)
            end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
            result_tostring = ','.join(map(str, values))
            point = {datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S.%f"): f"每次获取耗时{end_time}毫秒，当前获取个数:{len(points)} 值：{values}"}
            logger.info(point)
            points.append(point)
            # print(len(points))
            if len(points) == COUNT:
                total_time = time.time_ns() - round(time.time_ns() - init_time, 4) / 1e6  # 毫秒
                result = f"{COUNT}次连接共耗时{total_time}毫秒即{round(total_time / 60000, 4)}分,平均连接一次耗时{round(total_time / COUNT, 4)}毫秒"
                logging.info(result)
                points.append(result)
                points.append(f"总共超时次数：{error_times}\n")
                with open("output_opc_ua_async.txt", "a", encoding='utf-8') as f:
                    f.write('\n'.join(map(str, points)))
            break
        except Exception as e:
            logger.error(e)
            error_times += 1
        finally:
            await opcua_async_client.disconnect()


if __name__ == "__main__":
    # get_value_subdata_change()
    get_values_sync_client()
    # asyncio.run(get_values_async_client())
