import logging.config
import os
import sys
from logging import Filter


# COUNT = 30000

class ExactLevelFilter(Filter):
    """
    精确级别过滤器，只允许指定级别的日志通过
    """

    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
    # log_path是存放日志的路径


log_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')
if not os.path.exists(log_path):
    os.mkdir(log_path)  # 如果不存在这个logs文件夹，就自动创建一个

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,  # 建议为 False，避免影响第三方库的日志
    'formatters': {
        # 日志格式
        'standard': {
            'format': '[%(asctime)s] [%(filename)s:%(lineno)d]'
                      '[%(levelname)s]- %(message)s'},
        'simple': {  # 简单格式
            'format': '%(levelname)s %(message)s'
        },
    },
    # 过滤
    'filters': {
        'debug_only': {
            '()': lambda: ExactLevelFilter(logging.DEBUG)
        },
        'info_only': {
            '()': lambda: ExactLevelFilter(logging.INFO)
        },
        'warning_only': {
            '()': lambda: ExactLevelFilter(logging.WARNING)
        },
        'error_only': {
            '()': lambda: ExactLevelFilter(logging.ERROR)
        },
        'critical_only': {
            '()': lambda: ExactLevelFilter(logging.CRITICAL)
        }
    },
    # 定义具体处理日志的方式
    'handlers': {
        # 默认记录所有日志
        'debug': {
            'level': 'DEBUG',
            # 'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(log_path, 'debug.log'),
            'maxBytes': 1024 * 1024 * 500,  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
            'filters': ['debug_only']
        },
        # 输出info日志
        'info': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            # 'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': os.path.join(log_path, 'info.log'),
            'maxBytes': 1024 * 1024 * 100,
            'backupCount': 5,
            'formatter': 'standard',
            'encoding': 'utf-8',  # 设置默认编码
            'filters': ['info_only']
        },
        'warning': {
            'level': 'WARNING',
            # 'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(log_path, 'warning.log'),
            'maxBytes': 1024 * 1024 * 500,  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
            'filters': ['warning_only']
        },

        # 输出错误日志
        'error': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            # 'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': os.path.join(log_path, 'error.log'),
            'maxBytes': 1024 * 1024 * 100,  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
            "filters": ["error_only"]
        },
        'critical': {
            'level': 'CRITICAL',
            # 'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(log_path, 'critical.log'),
            'maxBytes': 1024 * 1024 * 500,  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'standard',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
            'filters': ['critical_only']
        },
        # 控制台输出
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    # 配置用哪几种 handlers 来处理日志
    'loggers': {
        # 类型 为 django 处理所有类型的日志， 默认调用
        'django': {
            'handlers': ['debug', 'console'],
            'level': 'DEBUG',
            'propagate': False
        },
        # # log 调用时需要当作参数传入
        'log': {
            'handlers': ['console', 'debug', 'info', 'warning', 'error', 'critical'],
            'level': 'INFO',
            'propagate': False
        },
        '': {
            'handlers': ['console', 'debug', 'info', 'warning', 'error', 'critical'],
            'level': 'WARNING',
            'propagate': False
        }
    }
}
logging.config.dictConfig(LOGGING)
logger = logging.getLogger('log')


def global_exception_handler(exc_type, exc_value, exc_traceback):
    """全局异常处理器"""
    logger.error(" 未捕获的异常发生",
                 exc_info=(exc_type, exc_value, exc_traceback))
    # 保持原始异常处理行为（可选）
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


# 设置全局异常钩子
sys.excepthook = global_exception_handler
# plc_opcua_url = "opc.tcp://192.168.1.88:4840"
# node_id = "ns=4;s=变量表"
#
# # opcua同步连接plc
# opcua_client = Client(plc_opcua_url)

# # opcua异步连接plc
# opcua_async_client = AsyncClient(plc_opcua_url)
#
# token = "kDKf6ACTpykjjlX-4TsgmpwcU1MAae7AY6wM91-10wv2UxDPlnY2qZyVfmDT5ld0ytD_w0IC4cRxVn4RuhzFzQ=="
# org = "my-org"
# url = "http://localhost:8086"
# bucket = "my-bucket"
# measurement = "experiment"
#
# influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
# write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
