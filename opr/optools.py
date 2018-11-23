# coding : utf-8
'''
--------------------------------------------------------------------
项目名：rwp
模块名：optools
该模块包含了本项目与业务化处理有关的函数
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
import os
import pickle as pk
from datetime import datetime, timedelta
import time
import logging

# 调用全局日志
logger = logging.getLogger('root')

def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def abstr_time(file_name, level='full'):
    '''从字符串提取时间'''
    if level == 'hour':
        result = file_name.split('_')[4][:10]
    elif level == 'minute':
        result = file_name.split('_')[4][:12]
    elif level == 'full':
        result = file_name.split('_')[4]

    return result


def drop_duplicate_station(dup_set):
    '''剔除标准时间轴下重复站点的文件源

    输入参数
    -------
    dup_set : `set`
        含有重复站点的文件名集合

    返回值
    -----
    `set` : 剔除了重复站点的文件名集合
    '''

    def get_station_id(file_name):
        '''根据文件名提取站点号'''
        station_id = file_name.split('_')[3]
        return station_id

    unique_set = set([])
    unique_id = set([])
    for fn in dup_set:
        station_id = get_station_id(fn)
        if station_id not in unique_id:
            unique_id.add(station_id)
            unique_set.add(fn)

    return unique_set


def strftime_to_datetime(strftime,mod='minute'):
    '''将时间字符串输出为datetime格式对象

    输入参数
    -------
    strftime : `str`
        时间字符串，精确到分钟，例如201809101306

    返回值
    -----
    `datetime` : 输入时间字符串对应的datetime对象
    '''
    year = int(strftime[:4])
    month = int(strftime[4:6])
    day = int(strftime[6:8])
    if mod == 'day':
        hour = 0
        minute = 0
    elif mod == 'hour':
        hour = int(strftime[8:10])
        minute = 0
    elif mod == 'minute':
        hour = int(strftime[8:10])
        minute = int(strftime[10:])

    return datetime(year, month, day, hour, minute)


def datetime_to_strftime(dt):
    '''将datetime转化为时间字符串

    输入参数
    -------
    dt : `datetime.datetime`
        datetime对象

    返回值
    -----
    `str` : 时间字符串
    '''
    year = str(dt.year)
    month = str(dt.month).zfill(2)
    day = str(dt.day).zfill(2)
    hour = str(dt.hour).zfill(2)
    minute = str(dt.minute).zfill(2)

    return ''.join([year,month,day,hour,minute])


def next_time_index(timestr):
    '''下一时次的时间字符串
    参数
    ----
    timestr : `str`
        时间字符串，精确到分钟级，例如201809101306
    返回
    ----
    `str`
        时间字符串，即输入时间字符串往后推6分钟的值
        例如输入值为201809101306，则返回值为201809101312
    '''
    year = int(timestr[:4])
    month = int(timestr[4:6])
    day = int(timestr[6:8])
    hour = int(timestr[8:10])
    minute = int(timestr[10:])

    this_time = datetime(year, month, day, hour, minute)
    time_delt = timedelta(minutes=6)
    next_time = this_time + time_delt

    return next_time.strftime('%Y%m%d%H%M')


def get_expect_time(preset_path):
    '''获取期望时次'''
    time_preset_pfn = preset_path + 'times.pk'
    try:
        time_preset = load_preset(time_preset_pfn)
    except:
        time_preset = None
    if time_preset:
        result = next_time_index(sorted(list(time_preset))[-1])
    else:
        std_index = standard_time_index()
        now = datetime.utcnow()
        for idx in std_index:
            if now >= strftime_to_datetime(idx):
                result = idx

    return result


def extract_curset(files, expect_time, preset_path):
    '''收集文件源（文件名）'''

    # 初始化当前处理集合，curset : current set
    curset = set([])

    # 设置前集路径
    today = get_today_date()
    time_preset_pfn = preset_path + 'times.pk'
    file_preset_pfn = preset_path + 'files.pk'

    # 检查前集是否存在，若不存在则初始化
    if not os.path.exists(file_preset_pfn):
        init_preset(file_preset_pfn)
    if not os.path.exists(time_preset_pfn):
        init_preset(time_preset_pfn)

    # 加载前集
    time_preset = load_preset(time_preset_pfn)
    file_preset = load_preset(file_preset_pfn)

    # 获取期望时次
    # expect_time = get_expect_time(preset_path)
    print('expecting: %s' % expect_time)
    logger.info(' expecting: %s' % expect_time)

    # （未处理）新集是当前全集减去前集
    newset = set(files) - file_preset

    for file in newset:
        file_time = abstr_time(file, level='minute')
        # 每一个文件匹配一个标准时间索引
        match_time = match_standard(file_time)
        # 若匹配的标准时次为期望时次，则加入curset，否则忽略
        if match_time == expect_time:
            curset.add(file)

    # 删除该时次重复的站
    curset = drop_duplicate_station(curset)
    print('real time received: {}'.format(len(curset)))
    logger.info(' real time received: {}'.format(len(curset)))

    # 达到时间阈值后返回该集合
    spent = datetime.utcnow() - strftime_to_datetime(expect_time)
    if  spent > timedelta(minutes=6):
        print('finally received: {}'.format(len(curset)))
        logger.info(' finally received: {}'.format(len(curset)))
        file_preset.update(curset)
        time_preset.add(expect_time)
        save_preset(time_preset, time_preset_pfn)
        save_preset(file_preset, file_preset_pfn)
        result =  curset
        if not result:
            # 若超时但结果为空集，说明该时次缺失，缺失标志改为True
            print('{} is missing.'.format(expect_time))
            logger.info(' {} is missing.'.format(expect_time))
        turn_time = True
    else:
        result = set([])
        turn_time = False

    return result, turn_time


def init_preset(path):
    '''初始化前集'''
    if not os.path.exists(path):
        with open(path, 'wb') as file_obj:
            pk.dump(set([]), file_obj)


def save_preset(preset, path):
    '''存储前集'''
    with open(path, 'wb') as file_obj:
        pk.dump(preset, file_obj)


def load_preset(path):
    '''加载前集'''
    with open(path, 'rb') as file_obj:
        try:
            preset = pk.load(file_obj)
        except EOFError:
            preset = set([])

    return preset


def standard_time_index():
    '''建立逐6分钟标准时间索引'''
    now = datetime.utcnow()
    year = str(now.year)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    date = ''.join([year, month, day])
    hours = [str(n).zfill(2) for n in range(24)]
    minutes = [str(n).zfill(2) for n in range(0, 60, 6)]

    # std : standard time
    stdt_index = []
    for hour in hours:
        for minute in minutes:
            stdt_index.append(''.join([date, hour, minute]))

    return tuple(stdt_index)


def match_standard(timestr):
    '''（规定格式的）任意时间字符串向标准时间索引的匹配

    输入参数
    -------
    timestr : `string`
        时间字符串，其长度为12，精确到分钟，例如:201809071453

    返回值
    -----
    `string`
        经匹配最近的标准时间索引
    '''
    try:
        assert len(timestr) == 12
    except AssertionError:
        raise ValueError('time str is not invalid.')

    std_index = standard_time_index()

    # ymdh : year-month-day-hour
    ymdh = timestr[:10]
    minute = int(timestr[10:])

    delt = set([])
    for index, time_str in enumerate(std_index):
        if ymdh == time_str[:10]:
            delt.add((abs(minute-int(time_str[10:])), index))

    min_index = min(delt)[1]

    return std_index[min_index]


def get_today_date():
    '''获得今日的日期字符串'''
    today = datetime.utcnow()
    today_str = today.strftime('%Y%m%d')
    return today_str


def get_yesterday_date():
    '''获得昨日的日期字符串'''
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime('%Y%m%d')


def delay_when_today_dir_missing(rootpath):
    '''检查今日目录是否存在'''

    today = get_today_date()

    inpath = rootpath + today + '/'
    while True:
        if os.path.exists(inpath):
            is_exist = True
            break
        else:
            time.sleep(10)

    return is_exist


def delay_when_data_dir_empty(path):
    while True:
        files = os.listdir(path)
        if files:
            is_empty = False
            print('Preparing...')
            time.sleep(5)
            break
        else:
            time.sleep(10)

    return is_empty


def main():
    '''主函数'''
    pass


if __name__ == '__main__':
    main()
