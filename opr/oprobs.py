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
from datetime import datetime
import traceback
import optools as opt
from algom.io import parse, save_as_json

with open('../config.json') as f:
    config = js.load(f)

# 根据传参选择运行模式
try:
    test_flag = sys.argv[1]
except IndexError:
    # 若无传参，则按业务化模式运行，加载业务化配置
    ROOT_PATH = config['data_source']
    LOG_PATH = config['parse']['oper']['log_path']
    SAVE_PATH = config['parse']['oper']['save_path']
    PRESET_PATH = config['parse']['oper']['preset_path']
else:
    if test_flag == 'test':
        # 若传参为'test'，则按普通测试模式运行，加载测试配置
        ROOT_PATH = config['data_source']
        LOG_PATH = config['parse']['test']['log_path']
        PRESET_PATH = config['parse']['test']['preset_path']
        SAVE_PATH = config['parse']['test']['save_path']
    elif test_flag == 'test_local':
        # 若传参为'test_local'，则按本地源模式运行，加载本地源配置
        ROOT_PATH = '/mnt/data14/liwt/test/source/'
        LOG_PATH = config['parse']['test']['log_path']
        PRESET_PATH = config['parse']['test']['preset_path']
        SAVE_PATH = config['parse']['test']['save_path']
    else:
        raise ValueError('Unkown flag')

opt.check_dir(LOG_PATH)
opt.check_dir(PRESET_PATH)
opt.check_dir(SAVE_PATH)


# 配置日志信息
import opr.log as log
logger = log.setup_custom_logger(LOG_PATH+'wprd','root')


def gather(curset, root_path):
    '''将同一标准时次所有站点的数据读取为json格式字符串

    输入参数
    -------
    curset : `set`
        当前时次的文件集合，集合内存储的是不含路径的待处理文件名。
    root_path : `str`
        待处理的源文件存储路径

    返回值
    -----
    result : `list`
        待输出的内容列表，该列表内嵌套了多个字典，其样式为：
        [
            {key1:value1,key2:value2, ... ,keyn:valuen},
            {key1:value1,key2:value2, ... ,keyn:valuen},
            {key1:value1,key2:value2, ... ,keyn:valuen},
            ...
            {key1:value1,key2:value2, ... ,keyn:valuen},
            {key1:value1,key2:value2, ... ,keyn:valuen}
        ]
    '''

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
    '''主要用于自动化处理ROBS文件

    输入参数
    -------
    rootpath : `str`
        数据源的根路径，其末尾不带日期文件夹路径
    outpath : `str`
        数据保存路径，其末尾不带日期文件夹路径
    '''

    opt.check_dir(outpath)

    # 初始化今日日期
    today = opt.get_today_date()

    # 清理 preset 文件
    try:
        os.remove(PRESET_PATH + 'files.pk')
        os.remove(PRESET_PATH + 'times.pk')
    except FileNotFoundError:
        pass

    # 判断日期是否更改的标识变量
    turn_day_switch = False

    opt.delay_when_today_dir_missing(rootpath)

    # 初始化首次处理的文件目录
    inpath = rootpath + today + '/'
    savepath = outpath + today + '/'
    opt.check_dir(savepath)

    # 若今日数据目录为空，则等待至其有值再继续
    opt.delay_when_data_dir_empty(inpath)

    # 初次启动标志
    initial = True

    expect_time = opt.get_expect_time(PRESET_PATH)
    turn_time = False

    # 今日时间对象
    dt_today = datetime.utcnow()

    while True:
        # 如果当前日期与上次记录不一致
        if opt.get_today_date() != today and turn_day_switch == False:
            turn_day_timestamp = time.time()
            turn_day_switch = True

        # 若当前时间比转日时间戳的间隔达到60秒，则改变文件夹目录，正式转日
        if turn_day_switch:
            turn_day_delay = time.time() - turn_day_timestamp
            if turn_day_delay >= 60:

                today = opt.get_today_date()
                logger.debug(' today: {}'.format(today))
                turn_day_switch = False

                try:
                    os.remove(PRESET_PATH + 'files.pk')
                    os.remove(PRESET_PATH + 'times.pk')
                except FileNotFoundError:
                    pass

                # 若今日的数据目录缺失，则等待至其到达再继续
                opt.delay_when_today_dir_missing(rootpath)

                inpath = rootpath + today + '/'
                logger.debug(' inpath: {}'.format(inpath))
                savepath = outpath + today + '/'
                opt.check_dir(savepath)

                # 若今日数据目录为空，则等待至其有值再继续
                opt.delay_when_data_dir_empty(inpath)

                dt_today = datetime.utcnow()

        files = os.listdir(inpath)

        if initial == True:
            print('{}: initialize.'.format(datetime.utcnow()))
            logger.info(' initialize.')
            initial = False
        else:
            pass

        if turn_time == True:
            expect_time = opt.get_expect_time(PRESET_PATH)

        curset, turn_time = opt.extract_curset(files,expect_time, dt_today,
                                               PRESET_PATH)

        if curset:
            print('{0}: processing: {1}'.format(datetime.utcnow(),expect_time))
            logger.info(' processing: {}'.format(expect_time))
            result_list = gather(curset, inpath)
            if result_list:
                save_as_json(result_list,
                             savepath + expect_time + '.json',
                             mod='multi')
                print('{}: finished.'.format(datetime.utcnow()))
                logger.info(' finished.')

        else:
            time.sleep(20)


if __name__ == '__main__':
    try:
        main(ROOT_PATH, SAVE_PATH)
    except:
        # 若出现异常，则打印回溯信息并记入日志
        traceback_message = traceback.format_exc()
        print(traceback_message)
        logger.error(traceback_message)
        exit()
