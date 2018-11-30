# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.opshr
该模块为业务化风切变程序
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
import shutil as st
from datetime import datetime, timedelta
import optools as opt
import algom.shear as shr


# 加载配置文件
with open('../config.json') as f:
    config = js.load(f)


# 判断测试模式还是业务模式
try:
    test_flag = sys.argv[1]
except IndexError:
    ROOT_PATH = config['mkgrd']['oper']['save_path']
    LOG_PATH = config['shear']['oper']['log_path']
    SAVE_PATH = config['shear']['oper']['save_path']
    PRESET_PATH = config['shear']['oper']['preset_path']
    BUFFER_PATH = config['shear']['oper']['buffer_path']
else:
    if test_flag == 'test1':
        ROOT_PATH = config['mkgrd']['oper']['save_path']
        LOG_PATH = config['shear']['test']['log_path']
        SAVE_PATH = config['shear']['test']['save_path']
        PRESET_PATH = config['shear']['test']['preset_path']
        BUFFER_PATH = config['shear']['test']['buffer_path']
    elif test_flag == 'test2':
        ROOT_PATH = config['mkgrd']['test']['save_path']
        LOG_PATH = config['shear']['test']['log_path']
        SAVE_PATH = config['shear']['test']['save_path']
        PRESET_PATH = config['shear']['test']['preset_path']
        BUFFER_PATH = config['shear']['test']['buffer_path']
    else:
        raise ValueError('Unkown flag')


# 检查创建目录
opt.check_dir(LOG_PATH)
opt.check_dir(PRESET_PATH)
opt.check_dir(SAVE_PATH)
opt.check_dir(BUFFER_PATH)


# 配置日志信息
import log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def main(rootpath, bufferpath, outpath):
    try:
        print('Initial')
        logger.info(' Initial')

        folds = os.listdir(rootpath)
        folds.sort()
        fold = folds[-1]
        opt.init_preset(PRESET_PATH+'shr.pk')

        while True:
            fold = sorted(os.listdir(rootpath))[-1]
            newfiles = opt.get_new_files(fold,ROOT_PATH,PRESET_PATH,'shr.pk')
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
                    # 为防止nc文件在写入的时候被下游程序读取并造成未知错误，
                    #   因此先将输出的文件保存到缓存文件夹，
                    #   在输出完成以后再将文件复制到目标文件夹，并清除缓存中的文件，
                    #   经过测试，shutil的复制时间在0.015s的时间量级，
                    #   因此下游程序程序在本程序复制文件期间读取数据的可能性微乎其微。
                    savepfn = savepath + fn.split('.')[0] + '.nc'
                    bufferpfn = bufferpath + fn.split('.')[0] + '.nc'
                    shr.full_wind_shear(foldpath + fn, bufferpfn)
                    st.copy(bufferpfn,savepfn)
                    os.remove(bufferpfn)
                    print('{0} finished'.format(fn))
                    logger.info(' {0} finished'.format(fn))

            time.sleep(5)
    except:
        traceback_message = traceback.format_exc()
        print(traceback_message)
        logger.info(traceback_message)
        exit()


if __name__ == '__main__':
    main(ROOT_PATH, BUFFER_PATH, SAVE_PATH)
