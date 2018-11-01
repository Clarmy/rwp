# coding : utf-8
'''
--------------------------------------------------------------------
项目名：wpr
模块名：log
日志模块
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
 李文韬   |   liwentao@mail.iap.ac.cn   |   https://github.com/Clarmy
--------------------------------------------------------------------
'''
import logging
from logging.handlers import TimedRotatingFileHandler

def setup_custom_logger(log_path,name):

    formatter = logging.Formatter(fmt='%(asctime)s:%(message)s')
    handler = TimedRotatingFileHandler(log_path, when='midnight')
    handler.suffix = '%Y%m%d.log'
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
