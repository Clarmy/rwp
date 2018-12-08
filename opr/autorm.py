# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.autorm
本模块用于自动删除历史格点数据
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
import sys
sys.path.append('..')

import shutil as st
from datetime import datetime, timedelta
import json as js
import time
import os

target_path, log_name = sys.argv[1], sys.argv[2]

with open('../config.json') as f:
    config = js.load(f)

log_path = config['remove']['log_path']+log_name+'/'

if not os.path.exists(log_path):
    os.makedirs(log_path)

import opr.log as log
logger = log.setup_custom_logger(log_path+'rm','root')

def main(target_path):
    while True:
        loop = True
        dt = datetime.utcnow() - timedelta(days=3)
        while loop:
            dt_str = dt.strftime('%Y%m%d')
            rmpath = target_path + dt_str
            if os.path.exists(rmpath):
                try:
                    st.rmtree(target_path + dt_str)
                except OSError as e:
                    print('{0}: failed to remove {1}, '\
                          'error: OSError, reason: {2}'.format(datetime.utcnow(),
                                                          dt_str, e))
                    logger.error(' failed to remove {0}, '\
                                 'error: OSError, reason: {1}'.format(dt_str, e))
                else:
                    print('{0}: successfully removed {1} dir'.format(
                                                    datetime.utcnow(),dt_str))
                    logger.info(' successfully removed {} dir'.format(dt_str))
            else:
                print('{0}: no dir to remove'.format(datetime.utcnow()))
                logger.info(' no dir to remove')
                loop = False
            dt = dt - timedelta(days=1)
        print('{0}: sleep for 1 hour'.format(datetime.utcnow()))
        logger.info(' sleep for 1 hour')
        time.sleep(3600)


if __name__ == '__main__':
    main(target_path)
