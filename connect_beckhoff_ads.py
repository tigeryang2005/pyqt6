import ctypes
import struct
import sys
from datetime import datetime
import os

import influxdb_client
import pyads

import time

from influxdb_client import WritePrecision, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from ctypes import sizeof

from settings import logger


class BatchingCallback(object):

    def success(self, conf: (str, str, str), data: str):
        # print(f"Written batch: {conf}, data: {data}")
        logger.info(f"Written batch: {conf}, data: {data}")

    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        # print(f"Cannot write batch: {conf}, data: {data} due: {exception}")
        logger.error(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        # print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")
        logger.error(f"Retryable error occurs for batch: {conf}, data: {data}")


# CX9020 PLC ip
PLC_AMS_NET_ID = '5.158.159.167.1.1'
result_name = "MAIN.result"
count1 = "count1"
count2 = "count2"
count3 = "count3"
count4 = "count4"

plc = pyads.Connection(PLC_AMS_NET_ID, pyads.PORT_TC3PLC1)

delta_time = 0

influxdb_token = "IqDwptUPXBLQh1VhBY7B4QtKJeQLfndnuXeVSRGYvbNxxIxQ8AzPTW_DBLKBJ97UY8xjwlt7USiex1p-Ku8M_Q=="
influxdb_org = "ada"
influxdb_bucket = "bucket"
influxdb_url = "http://localhost:8086"
measurement = "experiment"
tag_location = "location"
tag_tianjin = "tianjin"
client = influxdb_client.InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
callback = BatchingCallback()
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
    # write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point, write_precision=WritePrecision.NS)
    try:
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point, write_precision=WritePrecision.NS)
        # write_api.flush()
    except Exception as e:
        logger.error(e)
    print(point)


def device_notification():
    atr = pyads.NotificationAttrib(ctypes.sizeof(Result), max_delay=50, cycle_time=0.5)
    plc.add_device_notification(result_name, atr, notification_callback)


if __name__ == "__main__":
    plc.open()
    # 锚定delta_time
    plc_current_time = plc.read_by_name("Main.currentTime")
    now_time = time.time_ns()
    delta_time = int(now_time / 10 - plc_current_time)

    # test_point = {
    #     "measurement": "experiment",
    #     "tags": {"location": "tianjin"},
    #     "fields": {"count1": 999},
    #     "time": int(time.time_ns())
    # }
    # try:
    #     write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=test_point)
    #     write_api.flush()  # 强制刷新缓冲区
    # except Exception as e:
    #     logger.error(e)
    device_notification()

    while True:
        pass
