from opcua import Client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from asyncua import Client as AsyncClient
import logging
import logging.config
import os

COUNT = 30000

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
        },
        # 控制台输出
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
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
        },
    },
    # 配置用哪几种 handlers 来处理日志
    'loggers': {
        # 类型 为 django 处理所有类型的日志， 默认调用
        'django': {
            'handlers': ['default', 'console'],
            'level': 'DEBUG',
            'propagate': False
        },
        # # log 调用时需要当作参数传入
        'log': {
            'handlers': ['error', 'info', 'console', 'debug'],
            'level': 'DEBUG',
            'propagate': False
        },
        '': {
            'handlers': ['error', 'info', 'debug', 'warn'],
            'level': 'DEBUG',
        }
    }
}
logging.config.dictConfig(LOGGING)
logger = logging.getLogger('log')
plc_opcua_url = "opc.tcp://192.168.1.88:4840"
node_id = "ns=4;s=变量表"

# opcua同步连接plc
opcua_client = Client(plc_opcua_url)

# opcua异步连接plc
opcua_async_client = AsyncClient(plc_opcua_url)

token = "kDKf6ACTpykjjlX-4TsgmpwcU1MAae7AY6wM91-10wv2UxDPlnY2qZyVfmDT5ld0ytD_w0IC4cRxVn4RuhzFzQ=="
org = "my-org"
url = "http://localhost:8086"
bucket = "my-bucket"
measurement = "experiment"

influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
