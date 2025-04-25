import os
import time
from datetime import datetime, timedelta
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from pymodbus.client import ModbusTcpClient
import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
import json

from Upper_Computer.settings import logger

token = "kDKf6ACTpykjjlX-4TsgmpwcU1MAae7AY6wM91-10wv2UxDPlnY2qZyVfmDT5ld0ytD_w0IC4cRxVn4RuhzFzQ=="
org = "my-org"
url = "http://localhost:8086"
bucket = "my-bucket"
measurement = "experiment"
# 连接PLC
plc_ip = '192.168.1.88'
plc_port = 502
client_plc = ModbusTcpClient(host=plc_ip, port=plc_port, timeout=1, retries=10)

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

COUNT = 30


def read_plc_modbus(_):
    points = []
    init_time = time.time_ns()
    logger.warning(111)
    logger.debug(222)
    logger.error(333)
    logger.info(444)
    logger.critical(555)
    if client_plc.connect():
        while True:
            try:
                start_time = time.time_ns()
                # 批量读取寄存器，例如从地址0x0000开始读取125个寄存器 没开线程
                result = client_plc.read_holding_registers(address=0, count=125, slave=1)  # 根据你的需求调整起始地址和数量
                # time.sleep(0.003)
                end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
                point = {datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"): f"每次获取耗时{end_time}毫秒，值：{result.registers[3]}"}
                points.append(point)
                print(point, len(points))

                if len(points) == COUNT:
                    print(f"共耗时{round((time.time_ns() - init_time) / 1e6, 4)}毫秒")
                    with open("output_modbus.txt", "w") as f:
                        for item in points:
                            for key, value in item.items():
                                data = json.dumps(value, ensure_ascii=False)
                                f.write(f"{key},{data}\n")
                    break
                # points = []
                # now_ns = int(datetime.now() .timestamp() * 1e9)
                # temp = list(range(125))
                # fields = dict(zip(temp, result.registers))
                # # for r in result.registers:
                # #     point = {
                # #         "measurement": measurement,
                # #         "tags": {"sensor": "汇川PLC"},
                # #         "fields": {"temperature": value, "转数": value / 2},
                # #         "time": now_ns
                # #     }
                # point = {
                #     "measurement": measurement,
                #     "tags": {"sensor": "汇川PLC"},
                #     "fields": fields,
                #     "time": now_ns
                # }
                # write_api.write(bucket=bucket, org=org, record=point, write_precision=WritePrecision.NS)
                # elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
                #
                # print(f"modbus获取一次耗时{elapsed}毫秒")  # 不开线程情况下约15毫秒  后面尝试eip、opcua协议
                # if not result.isError():
                #     # if 0 not in result.registers:
                #     print("读取成功：", result.registers)
                # else:
                #     print("读取失败：", result)
                # 计算耗时并等待剩余时间

                # sleep_time = max(0, 1 * 1e9 - elapsed)  # 确保不累积延迟
                # if sleep_time > 0:
                #     time.sleep(sleep_time * 1e9)
                #     print(f"sleep time:{sleep_time}纳秒")
            except Exception as e:
                print("报错信息", e)
            finally:
                # 确保在程序结束时关闭连接
                client_plc.close()
    else:
        print('Plc connection failed.')


if __name__ == '__main__':
    # 多进程
    # pool_size = os.cpu_count() * 2
    # with Pool(pool_size) as pool:
    #     # while True:
    #     pool.map(read_plc, list(range(1)))

    # 多线程
    # with ThreadPoolExecutor(max_workers=100) as executor:
    #     # while True:
    #     executor.map(read_plc, [1])
    logger.error(133)
    read_plc_modbus(None)
