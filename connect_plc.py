import time

from pymodbus.client import ModbusTcpClient

if __name__ == '__main__':
    # 连接PLC
    plc_ip = '192.168.18.49'
    plc_port = 502
    client_plc = ModbusTcpClient(plc_ip, port=plc_port)

try:
    if client_plc.connect():
        while True:
            start_time = time.time_ns()
            # 批量读取寄存器，例如从地址0x0000开始读取10个寄存器 没开线程
            result = client_plc.read_holding_registers(address=0x0000, count=10, slave=0)  # 根据你的需求调整起始地址和数量
            if not result.isError():
                print("读取成功：", result.registers)
            else:
                print("读取失败：", result)
            # 计算耗时并等待剩余时间
            elapsed = (time.time_ns() - start_time)

            print(f"modbus获取一次耗时{round(elapsed / 1e6, 4)}毫秒")
            # sleep_time = max(0, 1 * 1e9 - elapsed)  # 确保不累积延迟
            # if sleep_time > 0:
            #     time.sleep(sleep_time * 1e9)
            #     print(f"sleep time:{sleep_time}纳秒")
    else:
        print('Plc connection failed.')
finally:
    # 确保在程序结束时关闭连接
    client_plc.close()
