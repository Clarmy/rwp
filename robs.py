# coding : utf-8
'''
module docstring
'''
import os
import json as js
import time
from datetime import datetime
import logging
import traceback
from logging.handlers import TimedRotatingFileHandler

from handler import proc_wrap
from optools import gather_res
from optools import init_preset
from optools import save_preset
from optools import load_preset

# 配置日志信息
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
FILE_HANDLER = TimedRotatingFileHandler('./log/wprd', when='midnight')
FILE_HANDLER.suffix = '%Y%m%d.log'
LOG_FORMAT = '%(asctime)s:%(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


def check_dir(path):
    '''检查并创建目录'''
    if not os.path.exists(path):
        os.makedirs(path)


def gather_robs(res_pool, itime, root_path):
    '''将同一标准时次所有站点的数据读取为json格式字符串'''

    result_list = []
    for file in res_pool[itime]:
        path_file = root_path + file
        single_dict = proc_wrap(path_file)
        result_list.append(js.dumps(single_dict))

    result_js = '\n'.join(result_list)

    return result_js


def get_today_date():
    '''获得今日的日期字符串'''
    today = datetime.utcnow()
    today_str = ''.join([str(today.year), str(today.month).zfill(2),
                         str(today.day).zfill(2)])
    return today_str


def delay_when_today_dir_missing(rootpath):
    '''检查今日目录是否存在'''

    today = get_today_date()

    inpath = rootpath + today + '/'
    while True:
        if os.path.exists(inpath):
            is_exist = True
            break
        time.sleep(10)

    return is_exist


def main(rootpath, outpath):
    '''主函数'''
    preset_path = './preset/robs.pk'

    check_dir(outpath)
    check_dir(preset_path)

    init_preset(preset_path)

    while True:

        delay_when_today_dir_missing(rootpath)

        today = get_today_date()
        inpath = rootpath + today + '/'
        savepath = outpath + today + '/'
        check_dir(savepath)

        preset = load_preset(preset_path)
        files = os.listdir(inpath)

        res_pool, preset, queue, has_new_task = gather_res(files, preset)
        LOGGER.info('队列中文件数:%i' % len(queue))

        if has_new_task:
            for itime in res_pool:
                LOGGER.info('正在生成:%s' % itime)
                js_str = gather_robs(res_pool, itime, inpath)
                with open(savepath+itime+'.json', 'w') as file_obj:
                    file_obj.write(js_str)

            save_preset(preset, preset_path)

        else:
            time.sleep(10)


if __name__ == '__main__':
    ROOT_PATH = '/mnt/data3/REALTIME_DATA/cmadata/RADR/WPRD/ROBS/'
    SAVE_PATH = '/mnt/data14/liwt/output/WPR/'
    try:
        main(ROOT_PATH, SAVE_PATH)
    except Exception:
        # 若抛出无法预期的异常，则在日志中记录该异常的提示信息
        TRACEBACK = traceback.format_exc()
        LOGGER.error('%s\n' % TRACEBACK)
