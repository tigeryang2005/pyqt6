import os
import time
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from pymodbus.client import ModbusTcpClient

# 连接PLC
plc_ip = '192.168.18.49'
plc_port = 502
client_plc = ModbusTcpClient(host=plc_ip, port=plc_port, timeout=1)


def read_plc(_):
    while True:
        try:
            if client_plc.connect():
                start_time = time.time_ns()
                # 批量读取寄存器，例如从地址0x0000开始读取125个寄存器 没开线程
                result = client_plc.read_holding_registers(address=0, count=125, slave=1)  # 根据你的需求调整起始地址和数量
                elapsed = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
                print(f"modbus获取一次耗时{elapsed}毫秒")  # 不开线程情况下约15毫秒  后面尝试eip、opcua协议
                if not result.isError():
                    # if 0 not in result.registers:
                    print("读取成功：", result.registers)
                else:
                    print("读取失败：", result)
                # 计算耗时并等待剩余时间

                # sleep_time = max(0, 1 * 1e9 - elapsed)  # 确保不累积延迟
                # if sleep_time > 0:
                #     time.sleep(sleep_time * 1e9)
                #     print(f"sleep time:{sleep_time}纳秒")
            else:
                print('Plc connection failed.')
        finally:
            # 确保在程序结束时关闭连接
            client_plc.close()


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

    read_plc(None)