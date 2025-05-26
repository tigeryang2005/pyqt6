import ctypes
import itertools
import threading
import time
# 优化2: 使用线程安全的deque代替queue
from collections import deque
from concurrent.futures import ThreadPoolExecutor

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


batch_size = 5000
MAX_WORKERS = 4  # 根据CPU核心数调整
max_queue_size = 100000  # 设置最大队列大小防止内存溢出
writer_running = True
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

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


class HighSpeedBuffer:
    def __init__(self):
        self._buffer = deque(maxlen=max_queue_size)
        self._lock = threading.Lock()
        self._counter = itertools.count()

    def put(self, item):
        with self._lock:
            self._buffer.append(item)
        if len(self._buffer) > max_queue_size * 0.9:
            logger.warning(" 队列接近上限，考虑调整批量大小或工作线程数")

    def get_batch(self, size):
        with self._lock:
            if len(self._buffer) == 0:
                return None
            batch = []
            for _ in range(min(size, len(self._buffer))):
                batch.append(self._buffer.popleft())  # 从左端取出并移除
            return batch


data_buffer = HighSpeedBuffer()


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
    point = create_point(value)
    data_buffer.put(point)

    # 性能统计
    global processed_count, last_log_time
    processed_count += 1
    current_time = time.time()
    if current_time - last_log_time >= 1.0:
        logger.info(f"队列长度: {len(data_buffer._buffer)}, 处理速度: {processed_count}/s")
        last_log_time = current_time
        processed_count = 0


def write_batch(batch, max_retries=2):
    for attempt in range(max_retries):
        try:
            with influxdb_client.InfluxDBClient(
                    url=influxdb_url,
                    token=influxdb_token,
                    org=influxdb_org,
                    timeout=10_000  # 减少超时时间
            ) as temp_client:
                write_api = temp_client.write_api(write_options=SYNCHRONOUS)
                write_api.write(
                    bucket=influxdb_bucket,
                    record=batch,
                    write_precision=WritePrecision.NS
                )
                return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f" 写入失败(已重试{max_retries}次): {e}")
                return False
            time.sleep(0.5 * (attempt + 1))


def writer_thread():
    while writer_running:
        try:
            batch = data_buffer.get_batch(batch_size)
            if batch:
                future = executor.submit(write_batch, batch)
                start = time.time()
                while not future.done():
                    if time.time() - start > 5.0:  # 5秒超时
                        logger.warning("  写入任务超时(5秒)")
                        break
                    time.sleep(0.1)  # 短暂休眠避免CPU占用过高
            else:
                time.sleep(0.001)

                # 每5秒记录状态
            if time.time() - last_log_time > 5:
                qsize = len(data_buffer._buffer)
                active_threads = sum(1 for t in executor._threads if t.is_alive())
                logger.info(
                    f"状态: 队列={qsize} 活跃线程={active_threads}/{MAX_WORKERS}"
                )

        except Exception as e:
            logger.error(f"  写入线程异常: {e}")
            time.sleep(1)  # 防止错误循环


def shutdown(handler1, handler2):
    global writer_running

    # 2. 停止写入线程
    writer_running = False
    # 2. 停止PLC通知（添加重试机制）
    for _ in range(3):  # 最多尝试3次
        try:
            plc.del_device_notification(handler1, handler2)
            break
        except Exception as e:
            logger.warning(f" 停止通知失败: {e}")
            time.sleep(0.1)
    # 3. 等待可能正在执行的回调完成
    time.sleep(2.0)  # 适当延长等待时间

    # 4. 关闭线程池

    executor.shutdown(wait=True)
    logger.info(" 已关闭线程池...")
    # 4. 处理剩余数据
    print("开始写入队列中剩余的数据...")
    remaining_count = 0
    batch = data_buffer.get_batch(batch_size)
    batch_len = len(batch)
    if batch_len > 0:
        write_batch(batch)  # 直接写入，不使用线程池
    remaining_count += batch_len
    print(f"已写入队列中剩余的 {batch_len} 条数据，队列还剩余 {len(data_buffer._buffer)} 条")

    # 3. 等待线程池任务完成
    if len(data_buffer._buffer) > 0:
        logger.warning(f" 超时关闭，剩余 {len(data_buffer._buffer)} 条数据未处理")

    return remaining_count


def device_notification():
    atr = pyads.NotificationAttrib(ctypes.sizeof(Result),
                                   trans_mode=pyads.ADSTRANS_SERVERONCHA,
                                   max_delay=50, cycle_time=0.5)
    notification_handler, user_handler = plc.add_device_notification(result_name, atr, notification_callback)
    return notification_handler, user_handler


if __name__ == "__main__":
    plc.open()

    # 初始化
    writer_running = True
    writer = threading.Thread(target=writer_thread, daemon=True)
    writer.start()

    # 锚定delta_time
    plc_current_time = plc.read_by_name(result_name, Result).currentTime
    delta_time = int(time.time_ns() - (plc_current_time * 10))

    # 订阅数据
    start_time = time.perf_counter()
    notification_handler, user_handler = device_notification()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("用户退出")
        duration = time.perf_counter() - start_time
        remaining = shutdown(notification_handler, user_handler)

        query_sql = f'''
                    from(bucket: "{influxdb_bucket}")
                      |> range(start: -5m)
                      |> filter(fn: (r) => r._measurement == "experiment")
                      |> filter(fn: (r) => r.location  == "tianjin")
                      |> filter(fn: (r) => r._field == "count1")
                    '''
        try:
            res = client.query_api().query(query_sql, org=influxdb_org)
            count = 0
            for r in res:
                count = len(r.records)
            print(f"最后处理剩余数据: {remaining} 条")
            print(f"查询到 {count} 条数据")

            print(f"总写入数据: {count} 条")
            print(f"运行时间: {duration:.2f}秒")
            print(f"平均QPS: {count / duration:.2f}")

        except Exception as e:
            print(f"查询出错: {e}")
        finally:
            # Cleanup
            client.close()
            plc.close()
