import ctypes
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import influxdb_client
import pyads
from influxdb_client import WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS, WriteOptions, WriteType

from settings import logger

# PLC 配置
PLC_AMS_NET_ID = '5.158.159.167.1.1'
RESULT_NAME = "MAIN.result"

# InfluxDB 配置
INFLUXDB_TOKEN = "09J0dxHzR_o0GHRlHbPX5GlXZ4YVwYIFXAWYtC6q0LO_BwKmsm2izZ3E6twobof6KzMFMOvUUbdd_N8txcAnNw=="
INFLUXDB_ORG = "ada"
INFLUXDB_BUCKET = "bucket"
INFLUXDB_URL = "http://localhost:8086"

# 数据字段常量
COUNT1 = "count1"
COUNT2 = "count2"
COUNT3 = "count3"
COUNT4 = "count4"
CURRENT_TIME = "currentTime"
MEASUREMENT = "experiment"
TAG_LOCATION = "location"
TAG_TIANJIN = "tianjin"

# 性能配置
BATCH_SIZE = 5_000
MAX_QUEUE_SIZE = 1000_000


class Result(ctypes.Structure):
    """PLC 数据结构定义"""
    _fields_ = [
        (COUNT1, ctypes.c_float),
        (COUNT2, ctypes.c_int32),
        (COUNT3, ctypes.c_int16),
        (COUNT4, ctypes.c_bool),
        (CURRENT_TIME, ctypes.c_int64)
    ]


class BatchingCallback(object):

    def success(self, conf: (str, str, str), data: str):
        # logger.debug(f"Written batch: {conf}, data: {data}")
        logger.debug(f"Written batch: {conf}")

    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        # print(f"Cannot write batch: {conf}, data: {data} due: {exception}")
        logger.error(f"Written batch: {conf},due:{exception}")

    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        # print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")
        logger.warning(f"Retryable error occurs for batch: {conf}, retry: {exception}")


class HighSpeedBuffer:
    """高性能线程安全缓冲区"""

    def __init__(self):
        self._buffer = deque(maxlen=MAX_QUEUE_SIZE)

    def put(self, item):
        """添加数据到缓冲区"""
        self._buffer.append(item)
        if len(self._buffer) > MAX_QUEUE_SIZE * 0.9:
            logger.warning(" 队列接近上限，请考虑调整批量大小或增加工作线程")

    def get_batch(self, size):
        """从缓冲区获取一批数据"""
        if not self._buffer:
            return None
        return [self._buffer.popleft() for _ in range(min(size, len(self._buffer)))]


class DataCollector:
    """主数据收集器"""

    def __init__(self):
        self.plc = pyads.Connection(PLC_AMS_NET_ID, pyads.PORT_TC3PLC1)
        self.client = influxdb_client.InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
            timeout=30000
        )
        self.write_options = WriteOptions(write_type=WriteType.batching,
                                          batch_size=BATCH_SIZE,  # 每批写入的数据点数量
                                          flush_interval=1000,  # 毫秒，批量写入的间隔时间
                                          jitter_interval=0,  # 毫秒，写入时间抖动
                                          retry_interval=5000,  # 毫秒，重试间隔
                                          max_retries=3,  # 最大重试次数
                                          max_retry_delay=180000,  # 毫秒，最大重试延迟
                                          max_close_wait=300000,  # 毫秒，关闭等待时间
                                          exponential_base=2  # 指数退避基数
                                          )
        self.data_buffer = HighSpeedBuffer()
        self.writer_running = True
        self.processed_count = 0
        self.last_log_time = time.time()
        self.delta_time = 0

    def create_point(self, value):
        """创建InfluxDB数据点"""
        point_fields = {}
        for field, _ in value._fields_:
            point_fields[field] = getattr(value, field)
        point_fields.pop(CURRENT_TIME)
        return {
            "measurement": MEASUREMENT,
            "tags": {TAG_LOCATION: TAG_TIANJIN},
            "fields": point_fields,
            "time": value.currentTime * 10 + self.delta_time
        }

    def notification_callback(self, handle, name, timestamp, value):
        """PLC数据通知回调"""
        point = self.create_point(value)
        self.data_buffer.put(point)

        # 性能统计
        self.processed_count += 1
        current_time = time.time()
        if current_time - self.last_log_time >= 1.0:
            logger.info(f" 当前队列长度: {len(self.data_buffer._buffer)},  处理速度: {self.processed_count} 条/秒")
            self.last_log_time = current_time
            self.processed_count = 0

    def write_batch(self, batch, max_retries=2):
        callback = BatchingCallback()
        """写入一批数据到InfluxDB"""
        for attempt in range(max_retries):
            try:
                with self.client.write_api(write_options=self.write_options,
                                           success_callback=callback.success,
                                           error_callback=callback.error,
                                           retry_callback=callback.retry) as write_api:
                    write_api.write(bucket=INFLUXDB_BUCKET,
                                    record=batch,
                                    write_precision=WritePrecision.NS)
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f" 写入失败(已重试{max_retries}次): {str(e)}")
                    return False
                time.sleep(0.5 * (attempt + 1))

    def writer_thread(self):
        """数据写入线程"""
        while self.writer_running:
            try:
                batch = self.data_buffer.get_batch(BATCH_SIZE)
                if batch:
                    self.write_batch(batch)
            except Exception as e:
                logger.error(f" 写入线程异常: {str(e)}")
                time.sleep(1)

    def shutdown(self, notification_handler, user_handler):
        """关闭资源"""
        self.writer_running = False

        # 停止PLC通知
        for _ in range(3):
            try:
                self.plc.del_device_notification(notification_handler, user_handler)
                break
            except Exception as e:
                logger.warning(f" 停止通知失败: {str(e)}")
                time.sleep(0.1)
        time.sleep(1.0)  # 等待回调完成

        # 处理剩余数据
        remaining_count = 0
        while True:
            batch = self.data_buffer.get_batch(BATCH_SIZE)
            if not batch:
                break
            self.write_batch(batch)
            remaining_count += len(batch)

        logger.info(f" 已处理剩余数据: {remaining_count}条")
        return remaining_count

    def device_notification(self):
        """设置PLC数据通知"""
        atr = pyads.NotificationAttrib(
            ctypes.sizeof(Result),
            trans_mode=pyads.ADSTRANS_SERVERONCHA,
            max_delay=50,
            cycle_time=0.1
        )
        decorated_callback = self.plc.notification(Result)(self.notification_callback)
        return self.plc.add_device_notification(RESULT_NAME, atr, decorated_callback)

    def run(self):
        """主程序运行"""
        self.plc.open()

        # 初始化时间差
        plc_current_time = self.plc.read_by_name(RESULT_NAME, Result).currentTime
        # TODO: File "D:\pythonProjects\Upper_computer\venv\Lib\site-packages\pyads\connection.py", line 661,
        #  in read_structure_by_name return dict_from_bytes(values, structure_def, array_size=array_size)
        #  plc_current_time1 = self.plc.read_structure_by_name(data_name=RESULT_NAME, structure_def=Result._fields_,
        #  array_size=2, structure_size=ctypes.sizeof(Result) ) print(plc_current_time, plc_current_time1)
        # File"D:\pythonProjects\Upper_computer\venv\Lib\site-packages\pyads\ads.py", line
        # 318, in dict_from_bytes
        # var, plc_datatype, size, str_len = item  # type: ignore
        # ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^
        # ValueError: not enough values to unpack(expected4, got 2)
        # 可能是从bytes到截取的时候 每次截取的长度不对
        # plc_current_time1 = self.plc.read_structure_by_name(data_name=RESULT_NAME,
        #                                                     structure_def=Result._fields_,
        #                                                     structure_size=ctypes.sizeof(Result),
        #                                                     )
        # print(plc_current_time, plc_current_time1)
        self.delta_time = int(time.time_ns() - (plc_current_time * 10))

        # 启动写入线程
        writer = threading.Thread(target=self.writer_thread, daemon=True)
        writer.start()

        # 订阅数据
        start_time = time.perf_counter()
        notification_handler, user_handler = self.device_notification()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("用户终止程序")
            duration = time.perf_counter() - start_time
            remaining = self.shutdown(notification_handler, user_handler)

            # 查询统计数据
            query_sql = f'''
                    from(bucket: "{INFLUXDB_BUCKET}")
                      |> range(start: -5h)
                      |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
                      |> filter(fn: (r) => r.{TAG_LOCATION} == "{TAG_TIANJIN}")
                      |> filter(fn: (r) => r._field == "{COUNT1}")
                      |> count()
                '''
            res = self.client.query_api().query(query_sql, org=INFLUXDB_ORG).to_values(
                columns=["_time", "_measurement", "_value"]
            )
            total_count = int(res[0][2])
            try:
                print(f"\n===== 运行统计 =====")
                print(f"最后处理剩余数据: {remaining}条")
                print(f"总写入数据量: {total_count}条")
                print(f"程序运行时间: {duration:.2f}秒")
                print(f"QPS: {total_count / duration:.2f}条/秒")

            except Exception as e:
                print(f"数据查询出错: {str(e)}")
            finally:
                self.client.close()
                self.plc.close()
                print("资源已释放，程序退出")


if __name__ == "__main__":
    collector = DataCollector()
    collector.run()
