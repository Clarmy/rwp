# coding : utf-8
import os
import json as js

import time
from collections import defaultdict
from datetime import datetime
import logging
import traceback

from handler import proc_wrap
from optools import gather_res
from optools import init_preset
from optools import save_preset
from optools import load_preset

from logging.handlers import TimedRotatingFileHandler
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


def today_dir_exist():
{
    // The tab key will cycle through the settings when first created
    // Visit http://wbond.net/sublime_packages/sftp/settings for help

    // sftp, ftp or ftps
    "type": "sftp",

    "sync_down_on_open": true,
    "sync_same_age": true,

    "host": "example.com",
    "user": "username",
    //"password": "password",
    //"port": "22",

    "remote_path": "/example/path/",
    //"file_permissions": "664",
    //"dir_permissions": "775",

    //"extra_list_connections": 0,

    "connect_timeout": 30,
    //"keepalive": 120,
    //"ftp_passive_mode": true,
    //"ftp_obey_passive_host": false,
    //"ssh_key_file": "~/.ssh/id_rsa",
    //"sftp_flags": ["-F", "/path/to/ssh_config"],

    //"preserve_modification_times": false,
    //"remote_time_offset_in_hours": 0,
    //"remote_encoding": "utf-8",
    //"remote_locale": "C",
    //"allow_config_upload": false,
}



def main(rootpath, outpath):
    '''主函数'''
    preset_path = './preset/robs.pk'

    check_dir(outpath)
    check_dir(preset_path)

    init_preset(preset_path)

    while True:
        # 由于目标目录是根据UTC组织的yyyymmdd格式字符串，因此可以根据这一规则手动生成目标终端目录
        today = datetime.utcnow()
        today_str = ''.join([str(today.year), str(today.month).zfill(2),
                             str(today.day).zfill(2)])
        inpath = rootpath + today_str + '/'
        savepath = outpath + today_str + '/'
        check_dir(savepath)

        # 死循环检查目标目录是否存在，直到存在才跳出循环
        while True:
            try:
                os.listdir(inpath)
                break
            # 若不存在，休眠10秒后重试
            except FileNotFoundError:
                time.sleep(10)


        preset = load_preset(preset_path)
        files = os.listdir(inpath)

        res_pool, preset, queue, has_new_task = gather_res(files, preset)
        LOGGER.info('队列中文件数:{}'.format(len(queue)))
        # LOGGER.info('已处理文件数:{}'.format(len(preset)))

        if has_new_task:
            for itime in res_pool:
                LOGGER.info('正在生成{0}'.format(itime))
                js_str = gather_robs(res_pool, itime, inpath)
                with open(savepath+itime+'.json', 'w') as file_obj:
                    file_obj.write(js_str)

            save_preset(preset, preset_path)

        else:
            time.sleep(10)


if __name__ == '__main__':
    ROOT_PATH = '/mnt/data3/REALTIME_DATA/cmadata/RADR/WPRD/ROBS/'
    SAVE_PATH = '/mnt/data14/aviation/pafs/code/baowen/PAMS/py3/data/operational/wprd/'
    try:
        main(ROOT_PATH, SAVE_PATH)
    except:
        # 若抛出无法预期的异常，则在日志中记录该异常的提示信息
        traceback_message = traceback.format_exc()
        LOGGER.error('{0}\n'.format(traceback_message))
