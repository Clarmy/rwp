# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
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

from opr.optools import extract_curset, standard_time_index
from opr.optools import init_preset, save_preset, load_preset
from opr.optools import check_dir, get_expect_time
from opr.optools import get_today_date, get_yesterday_date
from opr.optools import delay_when_today_dir_missing
from opr.optools import delay_when_data_dir_empty
from algom.io import parse, save_as_json

with open('../config.json') as f:
    config = js.load(f)

try:
    test_flag = sys.argv[1]
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


def gather(curset, root_path):
    '''将同一标准时次所有站点的数据读取为json格式字符串'''

    result_list = []
    for file in sorted(list(curset)):
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

    # 清理 preset 文件
    try:
        os.remove(PRESET_PATH + 'files.pk')
        os.remove(PRESET_PATH + 'times.pk')
    except FileNotFoundError:
        pass

    # 判断日期是否更改的标识变量
    turn_day_switch = False

    delay_when_today_dir_missing(rootpath)

    # 初始化首次处理的文件目录
    inpath = rootpath + today + '/'
    savepath = outpath + today + '/'
    check_dir(savepath)

    # 若今日数据目录为空，则等待至其有值再继续
    delay_when_data_dir_empty(inpath)

    # 初次启动标志
    initial = True

    expect_time = get_expect_time(PRESET_PATH)
    turn_time = False
    while True:
        # 如果当前日期与上次记录不一致
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

        files = os.listdir(inpath)

        if initial == True:
            print('initialize.')
            logger.info(' initialize.')
            initial = False
        else:
            pass

        if turn_time == True:
            expect_time = get_expect_time(PRESET_PATH)

        curset, turn_time = extract_curset(files,expect_time, PRESET_PATH)

        if curset:
            print('processing: {}'.format(expect_time))
            logger.info(' processing: {}'.format(expect_time))
            # t0 = time.time()
            # for fn in sorted(list(curset)):
            result_list = gather(curset, inpath)
            # print(time.time()-t0)
            if result_list:
                save_as_json(result_list,
                             savepath + expect_time + '.json',
                             mod='multi')
                print('finished')
                logger.info(' finished')

        else:
            time.sleep(20)


if __name__ == '__main__':
    # ROOT_PATH = config['data_source']
    try:
        main(ROOT_PATH, SAVE_PATH)
    except:
        traceback_message = traceback.format_exc()
        print(traceback_message)
        logger.info(traceback_message)
        exit()
