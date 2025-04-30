import asyncio
import logging
import time
from datetime import datetime

from influxdb_client import WritePrecision
from opcua.tools import SubHandler

from settings import node_id, measurement, write_api, bucket, org, opcua_client, opcua_async_client, \
    logger, COUNT


class SubDataChangeHandler(SubHandler):
    def datachange_notification(self, node, val, data):
        output = f"{node.nodeid}: {val}"
        logger.info(output)


def get_value_subdata_change():
    # 创建订阅监听数据变化
    opcua_client.connect()

    sub = opcua_client.create_subscription(0.1, SubDataChangeHandler())
    nodes = opcua_client.get_node(node_id).get_children()
    points = sub.subscribe_data_change(nodes, queuesize=1000)


def get_values_sync_client():
    opcua_client.connect()
    nodes = opcua_client.get_node(node_id).get_children()
    logger.info(nodes)
    values = opcua_client.get_values(nodes)
    logger.info(values)
    time.sleep(0.3)


# 异步每次24ms 但是plc会返回too many sessions 拒绝请求
async def get_values_async_client():
    start_time = time.time_ns()
    await opcua_async_client.connect()

    nodes = await opcua_async_client.get_node(node_id).get_children()
    values = await opcua_async_client.read_values(nodes)
    end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
    result_tostring = ','.join(map(str, values))
    point = {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"): f"每次获取耗时{end_time}毫秒 值：{values}"}
    logger.info(point)


async def main_loop():
    """主循环管理"""
    while True:
        try:
            await get_values_async_client()
        except Exception as e:
            logger.error(f"主循环异常: {str(e)}")
        await asyncio.sleep(1)  # 控制轮询频率


if __name__ == "__main__":
    # get_value_subdata_change()
    # while True:
    #     get_values_sync_client()
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("程序终止")
