# coding : utf-8
'''
--------------------------------------------------------------------
项目名：wpr
模块名：opr.oprobs
本模块用于自动化解码风廓线雷达实时观测资料（ROBS）
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''

import sys
sys.path.append('..')

import os
import json as js
import time
from datetime import datetime, timedelta
import traceback

from opr.optools import gather_res, standard_time_index
from opr.optools import init_preset, save_preset, load_preset
from opr.optools import check_dir
from opr.optools import get_today_date, get_yesterday_date
from opr.optools import delay_when_today_dir_missing
from opr.optools import delay_when_data_dir_empty
from algom.io import parse, save_as_json

with open('../config.json') as f:
    config = js.load(f)

from sys import argv

try:
    test_flag = argv[1]
except IndexError:
    ROOT_PATH = config['data_source']
    LOG_PATH = config['parse']['oper']['log_path']
    SAVE_PATH = config['parse']['oper']['save_path']
    PRESET_PATH = config['parse']['oper']['preset_path']
else:
    if test_flag == 'test':
        ROOT_PATH = config['data_source']
        LOG_PATH = config['parse']['test']['log_path']
        PRESET_PATH = config['parse']['test']['preset_path']
        SAVE_PATH = config['parse']['test']['save_path']
    elif test_flag == 'test_local':
        ROOT_PATH = '/mnt/data14/liwt/test/source/'
        LOG_PATH = config['parse']['test']['log_path']
        PRESET_PATH = config['parse']['test']['preset_path']
        SAVE_PATH = config['parse']['test']['save_path']
    else:
        raise ValueError('Unkown flag')

check_dir(LOG_PATH)
check_dir(PRESET_PATH)
check_dir(SAVE_PATH)


# 配置日志信息
import opr.log as log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def gather_robs(res_pool, itime, root_path):
    '''将同一标准时次所有站点的数据读取为json格式字符串'''

    result_list = []
    for file in res_pool[itime]:
        path_file = root_path + file
        single_dict = parse(path_file)
        if not single_dict:
            return None
        else:
            result_list.append(single_dict)

    return result_list


def main(rootpath, outpath):
    '''主函数'''

    check_dir(outpath)
    # 初始化今日日期
    today = get_today_date()

    preset_pfn = PRESET_PATH + 'robs.%s.pk' % today
    init_preset(preset_pfn)

    # 判断日期是否更改的标识变量
    turn_day_switch = False

    delay_when_today_dir_missing(rootpath)

    # 初始化首次处理的文件目录
    inpath = rootpath + today + '/'
    savepath = outpath + today + '/'
    check_dir(savepath)

    # 若今日数据目录为空，则等待至其有值再继续
    delay_when_data_dir_empty(inpath)

    # 建立当天的标准时间索引
    STD_INDEX = standard_time_index()

    # 初次启动标志
    initial = True

    while True:
        # 如果当前日期与上次记录不一致，则建立转日时间戳
        if get_today_date() != today and turn_day_switch == False:
            turn_day_timestamp = time.time()
            turn_day_switch = True

        # 若当前时间比转日时间戳的间隔达到200秒，则改变文件夹目录，正式转日
        if turn_day_switch:
            turn_day_delay = time.time() - turn_day_timestamp
            if turn_day_delay >= 200:

                turn_day_switch = False

                today = get_today_date()
                preset_pfn = PRESET_PATH + 'robs.%s.pk' % today
                init_preset(preset_pfn)

                # 若今日的数据目录缺失，则等待至其到达再继续
                delay_when_today_dir_missing(rootpath)

                inpath = rootpath + today + '/'
                savepath = outpath + today + '/'
                check_dir(savepath)

                # 若今日数据目录为空，则等待至其有值再继续
                delay_when_data_dir_empty(inpath)

                # 建立当天的标准时间索引
                STD_INDEX = standard_time_index()

        preset = load_preset(preset_pfn)
        files = os.listdir(inpath)

        if initial == True:
            print('initialize.')
            logger.info(' initialize.')
        else:
            pass

        result = gather_res(files, preset,
            STD_INDEX,LOG_PATH,PRESET_PATH,initial)

        # 处理完成一次以后关闭initial标识
        initial = False

        res_pool = result['res_pool']
        preset = result['preset']
        has_new_task = result['has_new_task']
        expect = result['expect']

        # 分割线
        logger.info(' '+'-'*29)
        print('-'*30)

        if has_new_task:
            if expect in res_pool.keys():
                for itime in sorted(res_pool.keys()):
                    logger.info(' processing: %s' % itime)
                    print('processing: %s' % itime)
                    result_list = gather_robs(res_pool, itime, inpath)
                    if result_list:
                        save_as_json(result_list,savepath+itime+'.json',mod='multi')
                    else:
                        continue

            save_preset(preset, preset_pfn)

        else:
            time.sleep(10)


if __name__ == '__main__':
    # ROOT_PATH = config['data_source']
    main(ROOT_PATH, SAVE_PATH)
