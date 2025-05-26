import ctypes
import threading
import time
# 优化2: 使用线程安全的deque代替queue
from collections import deque

import influxdb_client
import pyads
from influxdb_client import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

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
client = influxdb_client.InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, timeout=30_000)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 优化1: 使用更大的批量大小
batch_size = 5000  # 从2000增加到5000
max_queue_size = 100000  # 设置最大队列大小防止内存溢出

data_buffer = deque()
buffer_lock = threading.Lock()

# 统计变量
processed_count = 0
last_log_time = time.time()


def create_point(value):
    return {
        "measurement": measurement,
        "tags": {tag_location: tag_tianjin},
        "fields": {
            count1: value.count1,
            count2: value.count2,
            count3: value.count3,
            count4: value.count4
        },
        "time": value.currentTime * 10 + delta_time
    }


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


def get_result_data(result):
    result_data = {count1: result.count1, count2: result.count2, count3: result.count3, count4: result.count4}
    return result_data


@plc.notification(Result)
def notification_callback(handle, name, timestamp, value):
    global processed_count, last_log_time

    point = create_point(value)

    with buffer_lock:
        data_buffer.append(point)
        processed_count += 1

        # 定期记录状态
    current_time = time.time()
    if current_time - last_log_time >= 1.0:
        with buffer_lock:
            buffer_size = len(data_buffer)
        logger.info(f"Buffer  size: {buffer_size}, Processed: {processed_count}/s")
        last_log_time = current_time
        processed_count = 0


def writer_thread():
    while True:
        with buffer_lock:
            if len(data_buffer) >= batch_size:
                batch = [data_buffer.popleft() for _ in range(min(batch_size, len(data_buffer)))]
            else:
                batch = None

        if batch:
            try:
                write_api.write(bucket=influxdb_bucket, record=batch, write_precision=WritePrecision.NS)
            except Exception as e:
                logger.error(f"Write  error: {e}")
                # 重试逻辑可以在这里添加

        # time.sleep(0.001)  # 短暂休眠避免CPU占用过高


# 启动写入线程
writer = threading.Thread(target=writer_thread, daemon=True)
writer.start()


def device_notification():
    atr = pyads.NotificationAttrib(ctypes.sizeof(Result),
                                   trans_mode=pyads.ADSTRANS_SERVERONCHA,
                                   max_delay=50, cycle_time=0.5)
    notification_handler, user_handler = plc.add_device_notification(result_name, atr, notification_callback)
    return notification_handler, user_handler


# def write_points():
#     # global write_num
#     points = []
#     for i in range(batch_size):
#         points.append(q.get())
#         # write_num += 1
#     try:
#         write_api.write(bucket=influxdb_bucket, record=points, write_precision=WritePrecision.NS)
#         # logger.info(f"{time.time_ns()},{q.qsize(), recevie_num, write_num}")
#         logger.info(f"{time.time_ns()},{q.qsize()}")
#     except Exception as e:
#         logger.error(e)


if __name__ == "__main__":
    plc.open()

    # 锚定delta_time
    plc_current_time = plc.read_by_name(result_name, Result).currentTime
    now_time = time.time_ns()
    delta_time = int(now_time - (plc_current_time * 10))

    # 订阅数据
    start_time = time.perf_counter()
    notification_handler, user_handler = device_notification()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("用户退出")
        plc.del_device_notification(notification_handler, user_handler)
        time.sleep(0.5)  # Allow any pending callbacks to complete

        print("开始写入队列中剩余的数据...")
        while True:
            with buffer_lock:
                if not data_buffer:
                    break
                batch = [data_buffer.popleft() for _ in range(min(batch_size, len(data_buffer)))]

            try:
                write_api.write(bucket=influxdb_bucket, record=batch, write_precision=WritePrecision.NS)
                write_api.flush()
                print(f"已写入 {len(batch)} 条数据，剩余 {len(data_buffer)} 条")
            except Exception as e:
                print(f"写入时出错: {e}")
            query_sql = f'''
                        from(bucket: "{influxdb_bucket}")
                          |> range(start: -20m)
                          |> filter(fn: (r) => r._measurement == "experiment")
                          |> filter(fn: (r) => r.location  == "tianjin")
                          |> filter(fn: (r) => r._field == "count1")
                        '''
            try:
                res = client.query_api().query(query_sql, org=influxdb_org)
                for r in res:
                    # print(r)
                    # print(r.columns)
                    # print(r.records)
                    count = len(r.records)
                print(f"查询到 {count} 条数据")
                print(
                    f"运行：{time.perf_counter() - start_time:.2f}秒, QPS: {round(count / (time.perf_counter() - start_time), 2)}")
            except Exception as e:
                print(f"查询出错: {e}")
        write_api.close()
        # Cleanup
        client.close()
        plc.close()
