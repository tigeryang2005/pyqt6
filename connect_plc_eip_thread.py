import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pylogix import PLC
from settings import logger, COUNT


class PLCBatchReader:
    def __init__(self, ip, slot, tags, batch_size=1000):
        self.ip = ip
        self.slot = slot
        self.tags = tags
        self.batch_size = batch_size
        self.buffer = []
        self.lock = threading.Lock()
        self.conn = PLC()
        self.error_times = 0

        # 建立持久连接
        self.conn.IPAddress = self.ip
        self.conn.ProcessorSlot = self.slot
        self.conn.conn.connect()

    def _read_batch(self):
        """优化后的批量读取方法"""
        try:
            with self.lock:
                responses = self.conn.Read(self.tags, count=5)
                ts = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}"
                res = ' '.join([ts] + [f"{r.TagName}:{r.Value}" for r in responses])
                return [res]
        except Exception as e:
            logger.error(f"PLC读取失败: {str(e)}")
            self.error_times += 1
            return [f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')},PLC读取第{self.error_times}失败"]

    def collect_data(self):
        """数据采集主循环"""
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for _ in range(COUNT):
                futures.append(executor.submit(self._read_batch))

                # 批量提交任务
                if len(futures) >= self.batch_size:
                    self._process_batch(futures)
                    futures = []

            # 处理剩余任务
            if futures:
                self._process_batch(futures)

    def _process_batch(self, futures):
        """批量结果处理"""
        batch_data = []
        for future in futures:
            results = future.result()
            batch_data.extend(results)

        # 原子化写入
        # with self.lock:
        self.buffer.extend(batch_data)
        if len(self.buffer) >= 1000:
            self._flush_buffer()

    def _flush_buffer(self):
        """高效文件写入"""
        if not self.buffer:
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        data_block = "\n".join(self.buffer)
        with open("output_opc_eip_thread_txt", "a", encoding='utf-8') as f:
            f.write(f"[{ts}] 批量记录\n{data_block}\n")
        self.buffer.clear()

    def shutdown(self):
        """资源释放"""
        self.conn.conn.close()


if __name__ == "__main__":
    # 配置参数
    plc_config = {
        'ip': "192.168.1.88",
        'slot': 1,
        'tags': ["count1", "count2", "count3", "count4", "count9"]
    }
    init_time = time.time_ns()
    # 初始化采集器
    collector = PLCBatchReader(**plc_config)

    try:
        # 启动数据采集
        collector.collect_data()
    finally:
        collector.shutdown()

    # 性能统计
    total_time = (time.time_ns() - init_time) / 1e6  # 毫秒
    avg_time = total_time / COUNT
    report = f"总耗时:{total_time:.4f}ms即{round(total_time / 60000, 4)}分 | 平均耗时:{avg_time:.4f}ms | 总连接次数:{COUNT} | 总读取失败次数:{collector.error_times} | 吞吐量:{COUNT / total_time * 1000:.4f}次/秒"
    logger.info(report)
    with open("output_opc_eip_thread_txt", "a", encoding='utf-8') as f:
        f.write(report)