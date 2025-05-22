import ctypes
import json
import struct
import sys
from datetime import datetime
import os

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
    ]


@plc.notification(Result)
def notification_callback(handle, name, timestamp, value):
    result = Result()
    result = value

    result_field = {}
    for field_name, field_type in result._fields_:
        value = getattr(result, field_name)
        result_field[field_name] = value

    point_time = int(timestamp.timestamp() * 1_000_000_000) + delta_time
    point = {
        "measurement": measurement,
        "tags": {tag_location: tag_tianjin},
        "fields": result_field,
        "time": point_time
    }
    try:
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)
        logger.info(f"{point_time}: {json.dumps(result_field, ensure_ascii=False)}")
    except Exception as e:
        logger.error(e)


def device_notification():
    atr = pyads.NotificationAttrib(ctypes.sizeof(Result), max_delay=50, cycle_time=0.5)
    notification_handler, user_handler = plc.add_device_notification(result_name, atr, notification_callback)
    return notification_handler, user_handler


# def write_points(points):
#     f = write_api.write(bucket=influxdb_bucket, record=points, write_precision=WritePrecision.NS)


if __name__ == "__main__":
    # logger.info("程序启动")
    # test_point = {
    #     "measurement": "experiment",
    #     "tags": {"location": "tianjin"},
    #     "fields": {"count1": -56.2999, "count2": 21177909, "count3": 9781, "count4": 0},
    #     "time": int(time.time_ns())
    # }
    # write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=test_point)
    # client.close()
    # try:
    #     logger.info("1")
    #     write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=test_point)
    #     write_api.flush()  # 强制刷新缓冲区
    #     logger.info(2)
    # except Exception as e:
    #     print(e)
    #     logger.error(e)

    plc.open()
    # 锚定delta_time
    plc_current_time = plc.read_by_name("Main.currentTime")
    now_time = time.time_ns()
    delta_time = int(now_time / 10 - plc_current_time)

    notification_handler, user_handler = device_notification()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        # logger.info("用户退出")
        plc.del_device_notification(notification_handler, user_handler)
        write_api.flush()
        write_api.close()
        client.close()
