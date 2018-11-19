# coding : utf-8
'''
--------------------------------------------------------------------
项目名：wpr
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
from collections import defaultdict
import ipdb
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


def drop_duplicate_station(old_res_dict):
    '''剔除标准时间轴下重复站点的文件源'''
    from collections import defaultdict

    def get_station_id(file_name):
        '''根据文件名提取站点号'''
        station_id = file_name.split('_')[3]
        return station_id

    new_res_dict = defaultdict(set)
    for std_time in old_res_dict:
        unique_station = set([])
        for file_name in old_res_dict[std_time]:
            station_id = get_station_id(file_name)
            if station_id not in unique_station:
                unique_station.add(station_id)
                new_res_dict[std_time].add(file_name)
            else:
                pass

    return new_res_dict


def parse_log_timestr(log_timestr):
    '''解析日志的时间字符串

    参数
    ----
    log_timestr : `str`
        读取日志的时间字符串，例如2018-09-18 09:26:13,067

    返回
    ----
    `datetime`
        输入时间字符串对应的datetime格式对象
    '''
    date, clock = log_timestr.split(' ')
    clock = clock.split(',')[0]

    year, month, day = date.split('-')
    hour, minute, second = clock.split(':')
    return datetime(int(year), int(month), int(day),
                    int(hour), int(minute), int(second))


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


def strftime_to_datetime(strftime):
    '''将时间字符串输出为datetime格式对象

    输入
    ----
    strftime : `str`
        时间字符串，精确到分钟，例如201809101306

    返回
    ----
    `datetime`
        输入时间字符串对应的datetime对象
    '''
    year = int(strftime[:4])
    month = int(strftime[4:6])
    day = int(strftime[6:8])
    hour = int(strftime[8:10])
    minute = int(strftime[10:])

    return datetime(year, month, day, hour, minute)


def is_timeout(time_index,LOG_PATH,threshold=360):
    '''判断是否超时
    若当前时间与最后一次处理记录之间的时间差超过阈值（默认360秒）则判定超时，
    若当前时刻滞后于所到文件时刻（收到了来自未来的文件），则最大时间阈值顺延10秒。

    输入参数
    -------


    返回值
    -----


    '''
    # 如果所到文件时次已经超过utc当前时刻，那么时间阈值也要相应地后延10秒
    if datetime.utcnow() < strftime_to_datetime(time_index):
        threshold += 10

    # 用日志信息获取历史处理记录，用以判断是否超时
    with open(LOG_PATH+'wprd') as log_obj:
        logtext = log_obj.readlines()

    # 从后往前扫描
    logtext.reverse()
    for line in logtext:
        try:
            # value 为已处理文件的文件名，它是日志中紧跟processing后面的一组数字串
            # 例如 201809170848，程序根据这一数字串来推算下一时次的数字串
            datetimestr, action, value = line.strip().split(': ')
        except ValueError:
            # 在非processing行时跳出此次循环
            continue
        if action == 'processing':
            # 若是processing行，则记录最后一次处理记录last_proc_time（本地时间）
            # 同时根据value值推算下一时次文件的变量名（UTC时间）
            last_proc_time = parse_log_timestr(datetimestr)
            # expect_time_index 即期望时次，它必须紧随上一个已处理时次。
            expect_time_index = next_time_index(value)
            break
    else:
        # 如果上述循环没有被break终止，说明当前日志文件中不存在processing行，\
        #     在初始建立日志文件时会出现这种情况这时候需要手动设置last_proc_time \
        #     和 expect_time_index 的值
        today = datetime.today()

        # 把last_proc_time设置为当天的00:00:00，即当天初始时间，通过这样的设置可以使\
        #     程序在当天任何时刻启动都能把当天的历史数据补齐。
        last_proc_time = datetime(today.year, today.month, today.day, 0, 0, 0)

        # 把 expect_time_index 设置为当前处理时次（UTC），
        #     可以使程序直接开始处理此时刻而无需排队。
        expect_time_index = time_index

    if expect_time_index == time_index:
        # 若期望时次与当前时次相吻合则判断该时次是否超时
        delt = datetime.utcnow() - last_proc_time
        print(' delt seconds: {}'.format(delt.seconds))
        logger.info(' delt seconds: {}'.format(delt.seconds))
        if delt.seconds >= threshold:
            result = True
        else:
            result = False
        return result
    else:
        # 若当前时次不是期望时次，则加入队列，不予计时
        print('in queque.')
        logger.info(' in queue.')
        result = False

    return result


def gather_res(file_name_lst, preset, STD_INDEX, LOG_PATH, PRESET_PATH,
               initial):
    '''收集文件源（文件名）'''
    res_dict = defaultdict(set)
    preset_path = PRESET_PATH + 'robs_time_index.pk'

    if not os.path.exists(preset_path):
        init_preset(preset_path)

    index_preset = load_preset(preset_path)

    # （未处理）新集是当前全集减去前集
    queue = set(file_name_lst) - preset

    # 对新集时间字符串进行匹配处理，匹配成功则添加到源字典中，匹配失败则忽略
    for file in queue:
        res_timestr = abstr_time(file, level='minute')

        # 每一个文件匹配一个标准时间索引，以标准索引为键集中在一起
        match_timestr = match_standard(res_timestr, STD_INDEX)

        # 若该标准时间索引在索引前集内，则忽略该时次
        if match_timestr not in index_preset:
            res_dict[match_timestr].add(file)
            preset.add(file)

    # 删除该时次重复的站
    res_dict = drop_duplicate_station(res_dict)

    # 不超时（7分钟以内）情况下到站不全(小于69个站点）的时次予以保留
    to_remove = set([])
    for time_index in res_dict:
        if time_index in index_preset:
            break
        station_is_enough = is_station_enough(res_dict, time_index)

        # 在到站不够且时间未超时时，剔除该时次，使其下一次再尝试
        if initial == True:
            # 初始启动程序，仅判断到站情况
            if not (station_is_enough):
                for file in res_dict[time_index]:
                    preset.remove(file)
                    to_remove.add(time_index)

            # 若文件的前集不删，则索引也要同步入前集
            else:
                index_preset.add(time_index)
        else:
            # 初始启动之后既判断到站情况也判断超时情况
            time_is_out = is_timeout(time_index,LOG_PATH)
            if not (station_is_enough | time_is_out):
                for file in res_dict[time_index]:
                    preset.remove(file)
                    to_remove.add(time_index)

            # 若文件的前集不删，则索引也要同步入前集
            else:
                index_preset.add(time_index)

    # 在遍历结束以后删除要素
    try:
        for remove in to_remove:
            res_dict.pop(remove)
    except UnboundLocalError:
        pass

    newset = set(res_dict.keys())

    # 若新集存在，启动任务标识
    if newset:
        has_new_task = True
    else:
        has_new_task = False

    save_preset(index_preset, preset_path)

    result = {'res_pool':res_dict,'preset':preset,'queue':queue,
              'has_new_task':has_new_task}

    return result


def is_station_enough(res_pool, timestr, threshold=69):
    '''判断文件源池中指定时次的站点数是否足够'''
    num = len(res_pool[timestr])
    print('{0}’s station num: {1}'.format(timestr, num))
    logger.info(' {0}’s station num: {1}'.format(
        timestr, num))
    if num < threshold:
        result = False
    else:
        result = True

    return result


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


def match_standard(timestr, STD_INDEX):
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
    assert len(timestr) == 12

    # ymdh : year-month-day-hour
    ymdh = timestr[:10]
    minute = int(timestr[10:])

    delt = set([])
    for index, time_str in enumerate(STD_INDEX):
        if ymdh == time_str[:10]:
            delt.add((abs(minute-int(time_str[10:])), index))

    min_index = min(delt)[1]

    return STD_INDEX[min_index]


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
