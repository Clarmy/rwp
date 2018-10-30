# coding : utf-8
'''
本模块用于自动化解码风廓线雷达实时观测资料（ROBS）
python=3.6
'''
import os
import json as js
import time
from datetime import datetime, timedelta
import logging
import traceback
from logging.handlers import TimedRotatingFileHandler

from handler import proc_wrap
from optools import gather_res
from optools import init_preset
from optools import save_preset
from optools import load_preset
from optools import check_dir
from optools import standard_time_index
from wprio import save_as_json


# 配置日志信息
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
FILE_HANDLER = TimedRotatingFileHandler('./log/wprd', when='midnight')
FILE_HANDLER.suffix = '%Y%m%d.log'
LOG_FORMAT = '%(asctime)s:%(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


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


def get_today_date():
    '''获得今日的日期字符串'''
    today = datetime.utcnow()
    today_str = today.strftime('%Y%m%d')
    return today_str


def get_yesterday_date():
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime('%Y%m%d')


def delay_when_today_dir_missing(rootpath):
    '''检查今日目录是否存在'''

    today = get_today_date()

    inpath = rootpath + today + '/'
    while True:
        if os.path.exists(inpath):
            # LOGGER.info(' delay for today dir missing: end')
            is_exist = True
            break
        else:
            LOGGER.info(' delay for today dir missing: retry')
            print(' delay for today dir missing: retry')
            print(today)
            time.sleep(10)

    return is_exist


def delay_when_data_dir_empty(path):
    while True:
        files = os.listdir(path)
        if files:
            # LOGGER.info(' is data dir empty: no')
            is_empty = False
            print('Preparing...')
            time.sleep(5)
            break
        else:
            LOGGER.info(' is data dir empty: yes')
            time.sleep(10)

    return is_empty


def main(rootpath, outpath):
    '''主函数'''

    check_dir(outpath)
    # 初始化今日日期
    today = get_today_date()

    preset_path = './preset/robs.%s.pk' % today
    init_preset(preset_path)

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
                preset_path = './preset/robs.%s.pk' % today
                init_preset(preset_path)

                # 若今日的数据目录缺失，则等待至其到达再继续
                delay_when_today_dir_missing(rootpath)

                inpath = rootpath + today + '/'
                savepath = outpath + today + '/'
                check_dir(savepath)

                # 若今日数据目录为空，则等待至其有值再继续
                delay_when_data_dir_empty(inpath)

                # 建立当天的标准时间索引
                STD_INDEX = standard_time_index()

        preset = load_preset(preset_path)
        files = os.listdir(inpath)

        res_pool, preset, queue, has_new_task = gather_res(files, preset,
            STD_INDEX)
        LOGGER.info(' file pool: %i' % len(queue))

        print('file pool: %i' % len(queue))
        print('='*30)

        if has_new_task:
            for itime in sorted(res_pool.keys()):
                LOGGER.info(' processing: %s' % itime)
                print('processing: %s' % itime)
                result_list = gather_robs(res_pool, itime, inpath)
                if result_list:
                    save_as_json(result_list,savepath+itime+'.json',mod='multi')
                else:
                    continue

            save_preset(preset, preset_path)

        else:
            time.sleep(10)


if __name__ == '__main__':
    ROOT_PATH = '/mnt/data3/REALTIME_DATA/cmadata/RADR/WPRD/ROBS/'
    SAVE_PATH = '/mnt/data14/liwt/output/WPR/'
    main(ROOT_PATH, SAVE_PATH)
