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

from Upper_Computer.settings import logger, COUNT

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


def read_plc_modbus(_):
    points = []
    init_time = time.time_ns()
    error_times = 0
    if client_plc.connect():

        while True:
            try:
                start_time = time.time_ns()
                # 批量读取寄存器，例如从地址0x0000开始读取125个寄存器 没开线程
                result = client_plc.read_holding_registers(address=0, count=125, slave=1)  # 根据你的需求调整起始地址和数量
                # time.sleep(0.003)
                end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
                result_tostring = ','.join(map(str, result.registers))
                point = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} 每次获取耗时{end_time}毫秒,当前获取个数：{len(points)} 值为:{result_tostring}"
                # point = {f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} 每次获取耗时{end_time}毫秒,
                # 值为:": result.registers} logger.debug(point)
                points.append(point)
                if len(points) == COUNT:
                    total_time = round(time.time_ns() - init_time, 4) / 1e6  # 毫秒
                    result = f"{COUNT}次连接共耗时{total_time}毫秒即{round(total_time/60000, 4)}分,平均连接一次耗时{round(total_time/COUNT, 4)}毫秒"
                    logger.info(result)
                    points.append(result)
                    points.append(f"总共超时次数：{error_times}\n")
                    with open("output_modbus.txt", "a", encoding='utf-8') as f:
                        f.write('\n'.join(map(str, points)))
                    break
            except Exception as e:
                logger.error(e)
                error_times += 1
            finally:
                # 确保在程序结束时关闭连接
                client_plc.close()
    else:
        print('Plc connection failed.')


if __name__ == '__main__':
    read_plc_modbus(None)
