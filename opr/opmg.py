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
import optools as opt
import algom.makegrid as mkg


# 加载配置文件
with open('../config.json') as f:
    config = js.load(f)


# 判断测试模式还是业务模式
try:
    test_flag = sys.argv[1]
except IndexError:
    ROOT_PATH = config['parse']['oper']['save_path']
    LOG_PATH = config['mkgrd']['oper']['log_path']
    SAVE_PATH = config['mkgrd']['oper']['save_path']
    PRESET_PATH = config['mkgrd']['oper']['preset_path']
else:
    if test_flag == 'test1':
        ROOT_PATH = config['parse']['oper']['save_path']
        LOG_PATH = config['mkgrd']['test']['log_path']
        SAVE_PATH = config['mkgrd']['test']['save_path']
        PRESET_PATH = config['mkgrd']['test']['preset_path']
    elif test_flag == 'test2':
        ROOT_PATH = config['parse']['test']['save_path']
        LOG_PATH = config['mkgrd']['test']['log_path']
        SAVE_PATH = config['mkgrd']['test']['save_path']
        PRESET_PATH = config['mkgrd']['test']['preset_path']
    else:
        raise ValueError('Unkown flag')


# 检查创建目录
opt.check_dir(LOG_PATH)
opt.check_dir(PRESET_PATH)
opt.check_dir(SAVE_PATH)


# 配置日志信息
import log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def main(rootpath, outpath):
    try:
        print('Initial')
        logger.info(' Initial')

        folds = os.listdir(rootpath)
        folds.sort()
        fold = folds[-1]
        opt.init_preset(PRESET_PATH+'mg.pk')

        while True:
            fold = sorted(os.listdir(rootpath))[-1]
            newfiles = opt.get_new_files(fold,ROOT_PATH,PRESET_PATH,'mg.pk')
            foldpath = rootpath + fold + '/'
            savepath = outpath + fold + '/'
            opt.check_dir(savepath)
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
                    mkg.full_interp(foldpath + fn, savepath=savepfn)
                    print('{0} finished'.format(fn))
                    logger.info(' {0} finished'.format(fn))

            time.sleep(5)
    except:
        traceback_message = traceback.format_exc()
        print(traceback_message)
        logger.info(traceback_message)
        exit()


if __name__ == '__main__':
    main(ROOT_PATH, SAVE_PATH)
