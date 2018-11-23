# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.opmg
该模块为业务化插值拼图程序
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
import sys
sys.path.append('..')

import os
import time
import json as js
import traceback
from datetime import datetime, timedelta

from optools import check_dir, standard_time_index
from optools import init_preset, save_preset, load_preset
from optools import strftime_to_datetime, datetime_to_strftime
from optools import load_preset, save_preset, init_preset
from algom.makegrid import full_interp

# 加载配置文件
with open('../config.json') as f:
    config = js.load(f)

# 判断测试模式还是业务模式
try:
    test_flag = sys.argv[1]
except IndexError:
    LOG_PATH = config['mkgrd']['oper']['log_path']
    SAVE_PATH = config['mkgrd']['oper']['save_path']
    PRESET_PATH = config['mkgrd']['oper']['preset_path']
else:
    if test_flag == 'test':
        LOG_PATH = config['mkgrd']['test']['log_path']
        SAVE_PATH = config['mkgrd']['test']['save_path']
        PRESET_PATH = config['mkgrd']['test']['preset_path']
    else:
        raise ValueError('Unkown flag')

# 检查创建目录
check_dir(LOG_PATH)
check_dir(PRESET_PATH)
check_dir(SAVE_PATH)

# 配置日志信息
import log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def get_new_files(fold,PRESET_PATH):
    preset = load_preset(PRESET_PATH+'mg.pk')
    path = ROOT_PATH + fold + '/'
    curset = set(os.listdir(path))
    diffset = curset - preset
    preset.update(diffset)
    save_preset(preset, PRESET_PATH+'mg.pk')
    diff = list(diffset)
    diff.sort()
    return diff


def main(rootpath, outpath):
    try:
        print('Initial')
        logger.info(' Initial')

        folds = os.listdir(rootpath)
        folds.sort()
        fold = folds[-1]
        init_preset(PRESET_PATH+'mg.pk')

        while True:
            folds = os.listdir(rootpath)
            folds.sort()
            fold = folds[-1]
            newfiles = get_new_files(fold,PRESET_PATH)
            foldpath = rootpath + fold + '/'
            savepath = outpath + fold + '/'
            check_dir(savepath)
            if newfiles:
                print('dir {0} has new file:'.format(fold))
                logger.info(' dir {0} has new file:'.format(fold))
                for fn in newfiles:
                    print('\t{0}'.format(fn))
                    logger.info(' {0}'.format(fn))
                print('processing...')
                logger.info(' processing...')
                for fn in newfiles:
                    savepfn = savepath + fn.split('.')[0] + '.nc'
                    full_interp(foldpath + fn, savepath=savepfn)
                    print('{0} finished'.format(fn))
                    logger.info(' {0} finished'.format(fn))

            time.sleep(5)
    except:
        traceback_message = traceback.format_exc()
        print(traceback_message)
        logger.info(traceback_message)
        exit()


if __name__ == '__main__':
    ROOT_PATH = config['parse']['oper']['save_path']
    main(ROOT_PATH, SAVE_PATH)
