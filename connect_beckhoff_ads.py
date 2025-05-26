import ctypes
import json
import struct
import sys
from datetime import datetime
import os
import queue

import influxdb_client
import pyads

import time
import asyncio
from influxdb_client import WritePrecision, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from ctypes import sizeof

from settings import logger

# CX9020 PLC ip
PLC_AMS_NET_ID = '5.158.159.167.1.1'
result_name = "MAIN.result"
count1 = "count1"
count2 = "count2"
count3 = "count3"
count4 = "count4"
current_time = "currentTime"

plc = pyads.Connection(PLC_AMS_NET_ID, pyads.PORT_TC3PLC1)

delta_time = 0

influxdb_token = "09J0dxHzR_o0GHRlHbPX5GlXZ4YVwYIFXAWYtC6q0LO_BwKmsm2izZ3E6twobof6KzMFMOvUUbdd_N8txcAnNw=="
influxdb_org = "ada"
influxdb_bucket = "bucket"
influxdb_url = "http://localhost:8086"
measurement = "experiment"
tag_location = "location"
tag_tianjin = "tianjin"
client = influxdb_client.InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=ASYNCHRONOUS)
write_num = 0
recevie_num = 0

q = queue.Queue(maxsize=5000)
batch_size = 2000
"""
import ctypes

class Person(ctypes.Structure):
    _fields_ = [
        ("age", ctypes.c_int),
        ("height", ctypes.c_float),
        ("name", ctypes.c_char * 20)  # 固定长度字符串
    ]
"""


class Result(ctypes.Structure):
    _fields_ = [
        (count1, ctypes.c_float),
        (count2, ctypes.c_int32),
        (count3, ctypes.c_int16),
        (count4, ctypes.c_bool),
        (current_time, ctypes.c_int64)
    ]


@plc.notification(Result)
def notification_callback(handle, name, timestamp, value):
    global recevie_num
    recevie_num = recevie_num + 1

    result_value = value
    result_field = {}
    for field_name, field_type in result_value._fields_:
        result_field[field_name] = getattr(result_value, field_name)
    point_time = result_field[current_time] * 10 + delta_time
    result_field.pop(current_time)

    point = {
        "measurement": measurement,
        "tags": {tag_location: tag_tianjin},
        "fields": result_field,
        "time": point_time
    }
    # logger.debug(f"{point_time},{json.dumps(point, ensure_ascii=False)}")
    q.put(point)
    if q.qsize() > batch_size:
        logger.info(f"{time.time_ns()},{q.qsize(), recevie_num, write_num}")
        write_points()


def device_notification():
    atr = pyads.NotificationAttrib(ctypes.sizeof(Result),
                                   trans_mode=pyads.ADSTRANS_SERVERONCHA,
                                   max_delay=50, cycle_time=0.5)
    notification_handler, user_handler = plc.add_device_notification(result_name, atr, notification_callback)
    return notification_handler, user_handler


def write_points():
    global write_num
    points = []
    for i in range(batch_size):
        points.append(q.get())
        write_num += 1
    try:
        write_api.write(bucket=influxdb_bucket, record=points, write_precision=WritePrecision.NS)
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    start_time = time.perf_counter()
    plc.open()

    result_data = plc.read_by_name(result_name, Result)
    # 锚定delta_time
    plc_current_time = result_data.currentTime
    now_time = time.time_ns()
    delta_time = int(now_time - (plc_current_time * 10))

    # 订阅数据
    notification_handler, user_handler = device_notification()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("用户退出")
        plc.del_device_notification(notification_handler, user_handler)
        time.sleep(0.5)  # Allow any pending callbacks to complete

        print("开始写入队列中剩余的数据...")
        processed_any = False
        while True:
            try:
                point = q.get(timeout=1.0)
                points = [point]
                for _ in range(min(batch_size - 1, q.qsize())):  # Get remaining available points
                    try:
                        points.append(q.get_nowait())
                    except queue.Empty:
                        break

                write_api.write(bucket=influxdb_bucket, record=points, write_precision=WritePrecision.NS)
                write_num += len(points)
                processed_any = True
                print(f"已写入 {len(points)} 条数据，剩余 {q.qsize()}  条")
            except queue.Empty:
                if processed_any:
                    print("队列已空，写入完成")
                else:
                    print("队列中无数据")
                break
            except Exception as e:
                logger.error(f" 写入时出错: {e}")
                break

        print(
            f"运行：{time.perf_counter() - start_time:.2f}秒，收到{recevie_num}条数据，写入{write_num}条数据，qps：{round(write_num / (time.perf_counter() - start_time), 2)}")

        # Cleanup
        write_api.flush()
        write_api.close()
        client.close()
        plc.close()
