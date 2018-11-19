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

from optools import check_dir, standard_time_index
from optools import init_preset, save_preset, load_preset
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


def main(rootpath, outpath):

    STD_INDEX = standard_time_index()
    STD_FILENAMES = [ntime + '.json' for ntime in STD_INDEX]
    folds = os.listdir(rootpath)
    folds.sort()
    fold = folds[-1]

    while True:

        # 如果STD_FILENAMES为空，则扫描新的文件夹，否则扫描原有文件夹
        if not STD_FILENAMES:
            STD_INDEX = standard_time_index()
            STD_FILENAMES = [ntime + '.json' for ntime in STD_INDEX]
            folds = os.listdir(rootpath)
            folds.sort()
            fold = folds[-1]

        foldpath = rootpath+fold+'/'
        files = os.listdir(foldpath)
        while True:
            if STD_FILENAMES[0] in files:
                print('dir {0} finds new'\
                      ' file:{1}'.format(fold,STD_FILENAMES[0]))
                logger.info('dir {0} finds new'\
                      ' file: {1}'.format(fold,STD_FILENAMES[0]))
                savepath = outpath + fold + '/'
                check_dir(savepath)
                savepfn = savepath + \
                            STD_FILENAMES[0].split('.')[0] + '.nc'
                full_interp(foldpath + STD_FILENAMES[0], savepath=savepfn)
                print('making grid from {0} has been finished.'.format(STD_FILENAMES[0]))
                logger.info('making grid from {0} has been finished.'.format(STD_FILENAMES[0]))
                STD_FILENAMES.pop(0)
            else:
                break

        print('dir {0} has no new file'.format(fold))
        logger.info('dir {0} has no new file'.format(fold))
        time.sleep(10)


if __name__ == '__main__':
    ROOT_PATH = config['parse']['oper']['save_path']
    main(ROOT_PATH, SAVE_PATH)
