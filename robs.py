# coding : utf-8
'''
--------------------------------------------------------------------
项目名：wpr
模块名：robs
本模块用于自动化解码风廓线雷达实时观测资料（ROBS）
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
 李文韬   |   liwentao@mail.iap.ac.cn   |   https://github.com/Clarmy
--------------------------------------------------------------------
'''

import os
import json as js
import time
from datetime import datetime, timedelta
import traceback

from handler import proc_wrap
from optools import gather_res, standard_time_index
from optools import init_preset, save_preset, load_preset
from optools import check_dir
from optools import get_today_date, get_yesterday_date
from optools import delay_when_today_dir_missing
from optools import delay_when_data_dir_empty
# from optools import is_initial
from wprio import save_as_json
import ipdb

from sys import argv

try:
    test_flag = argv[1]
except IndexError:
    LOG_PATH = './log/'
    SAVE_PATH = '/mnt/data14/liwt/output/WPR/'
    PRESET_PATH = './preset/'
else:
    if test_flag == 'test':
        LOG_PATH = '/mnt/data14/liwt/test/log/'
        PRESET_PATH = '/mnt/data14/liwt/test/preset/'
        SAVE_PATH = '/mnt/data14/liwt/test/output/'
    else:
        print('Unkown flag')
        exit()

check_dir(LOG_PATH)
check_dir(PRESET_PATH)
check_dir(SAVE_PATH)


# 配置日志信息
import log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def gather_robs(res_pool, itime, root_path):
    '''将同一标准时次所有站点的数据读取为json格式字符串'''

    result_list = []
    for file in res_pool[itime]:
        path_file = root_path + file
        single_dict = proc_wrap(path_file)
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

        # 若当前时间比转日时间戳的间隔达到300秒，则改变文件夹目录，正式转日
        if turn_day_switch:
            turn_day_delay = time.time() - turn_day_timestamp
            if turn_day_delay >= 300:

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
        queue = result['queue']
        has_new_task = result['has_new_task']

        # 总文件池
        logger.info(' file pool: %i' % len(queue))
        print('file pool: %i' % len(queue))

        # 分割线
        logger.info(' '+'-'*29)
        print('-'*30)

        if has_new_task:
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
    ROOT_PATH = '/mnt/data3/REALTIME_DATA/cmadata/RADR/WPRD/ROBS/'
    main(ROOT_PATH, SAVE_PATH)
