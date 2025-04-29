import time
from datetime import datetime

from pylogix import PLC

from Upper_Computer.settings import logger, COUNT

# 建立连接
with PLC() as plc:
    plc.IPAddress = "192.168.1.88"  # EASY521的IP
    plc.ProcessorSlot = 1           # 模块槽号（通常为1）
    plc.conn.connect()

    # 写入数据（示例：向PLC的Output1标签写入值）
    # plc.Write("Output1", 123.45)

    # 读取数据（示例：读取PLC的Input1标签）
    init_time = time.time_ns()
    error_times = 0
    points = []
    max_retry_times = 10
    while True and error_times < max_retry_times:
        try:
            start_time = time.time_ns()
            responses = plc.Read(["count1", "count2", "count3", "count4", "count9"], count=5)
            end_time = round((time.time_ns() - start_time) / 1e6, 4)  # 纳秒换算成毫秒
            values = [r.Value for r in responses]
            result_tostring = ','.join(map(str, values))
            point = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} 每次获取耗时{end_time}毫秒,当前获取个数:{len(points)} 值为:{result_tostring}"
            points.append(point)
            if len(points) == COUNT:
                total_time = round(time.time_ns() - init_time, 4) / 1e6  # 毫秒
                result = f"{COUNT}次连接共耗时{total_time}毫秒即{round(total_time / 60000, 4)}分,平均连接一次耗时{round(total_time / COUNT, 4)}毫秒"
                logger.info(result)
                points.append(result)
                points.append(f"总共超时次数：{error_times}\n")
                with open("output_opc_eip.txt", "a", encoding='utf-8') as f:
                    f.write('\n'.join(map(str, points)))
                break
        except Exception as e:
            logger.error(e)
            error_times += 1
        finally:
            plc.conn.close()
